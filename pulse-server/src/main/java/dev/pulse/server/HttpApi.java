package dev.pulse.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

/**
 * HTTP/JSON API on the JDK's built-in server, one virtual thread per request.
 *
 * <pre>
 * POST /v1/jobs                    submit (idempotent on "id")
 * POST /v1/jobs/poll               claim one due job with a lease
 * POST /v1/jobs/{id}/heartbeat     extend a lease
 * POST /v1/jobs/{id}/ack           mark done
 * POST /v1/jobs/{id}/fail          report failure (retry with backoff, or DLQ)
 * GET  /v1/jobs/{id}               inspect a job
 * GET  /v1/dlq                     list dead jobs
 * GET  /healthz
 * </pre>
 *
 * Lease operations answer 409 {@code lease_lost} when the caller no longer holds the lease.
 */
public final class HttpApi {

    private static final System.Logger LOG = System.getLogger(HttpApi.class.getName());
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9._-]{1,128}");
    private static final int MAX_BODY_BYTES = 256 * 1024;
    private static final String JOBS = "/v1/jobs/";

    private final JobStore store;
    private final long defaultLeaseMillis;

    public HttpApi(JobStore store, long defaultLeaseMillis) {
        this.store = store;
        this.defaultLeaseMillis = defaultLeaseMillis;
    }

    public HttpServer start(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 1024);
        server.createContext("/", this::handle);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        return server;
    }

    private void handle(HttpExchange ex) throws IOException {
        try {
            route(ex);
        } catch (ApiException e) {
            send(ex, e.status, Map.of("error", e.code, "message", e.getMessage()));
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            send(ex, 400, Map.of("error", "bad_json", "message", "request body is not valid JSON"));
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.ERROR, "request failed: " + ex.getRequestURI(), e);
            send(ex, 500, Map.of("error", "internal", "message", "internal error"));
        } finally {
            ex.close();
        }
    }

    private void route(HttpExchange ex) throws IOException {
        String method = ex.getRequestMethod();
        String path = ex.getRequestURI().getPath();

        if (method.equals("GET") && path.equals("/healthz")) {
            send(ex, 200, Map.of("status", "ok"));
        } else if (method.equals("POST") && path.equals("/v1/jobs")) {
            submit(ex);
        } else if (method.equals("POST") && path.equals("/v1/jobs/poll")) {
            poll(ex);
        } else if (method.equals("GET") && path.equals("/v1/dlq")) {
            dlq(ex);
        } else if (path.startsWith(JOBS)) {
            String[] parts = path.substring(JOBS.length()).split("/");
            String id = parts[0];
            if (parts.length == 1 && method.equals("GET")) {
                getJob(ex, id);
            } else if (parts.length == 2 && method.equals("POST")) {
                switch (parts[1]) {
                    case "heartbeat" -> heartbeat(ex, id);
                    case "ack" -> ack(ex, id);
                    case "fail" -> fail(ex, id);
                    default -> throw new ApiException(404, "not_found", "no such route");
                }
            } else {
                throw new ApiException(404, "not_found", "no such route");
            }
        } else {
            throw new ApiException(404, "not_found", "no such route");
        }
    }

    private void submit(HttpExchange ex) throws IOException {
        JsonNode b = body(ex);
        String type = requireText(b, "type");
        String id = b.hasNonNull("id") ? b.get("id").asText() : UUID.randomUUID().toString();
        if (!ID.matcher(id).matches()) {
            throw new ApiException(400, "invalid_id", "id must match [A-Za-z0-9._-]{1,128}");
        }
        long now = System.currentTimeMillis();
        long runAt = b.hasNonNull("runAt") ? b.get("runAt").asLong() : now;
        int maxAttempts = (int) clamp(b.path("maxAttempts").asLong(5), 1, 100);
        int priority = b.path("priority").asInt(0);
        String payload = b.hasNonNull("payload") ? JSON.writeValueAsString(b.get("payload")) : "null";

        Job job = new Job(id, type, payload, Job.READY, runAt, priority, 0, maxAttempts,
                null, null, 0, null, now);
        JobStore.SubmitResult result = store.submit(job);
        send(ex, result.created() ? 201 : 200, view(result.job(), false));
    }

    private void poll(HttpExchange ex) throws IOException {
        JsonNode b = body(ex);
        String workerId = requireText(b, "workerId");
        Set<String> types = new HashSet<>();
        if (b.has("types")) {
            for (JsonNode t : b.get("types")) {
                types.add(t.asText());
            }
        }
        long leaseMs = clamp(b.path("leaseMs").asLong(defaultLeaseMillis), 1_000, 900_000);

        Optional<Job> leased = store.lease(workerId, types, leaseMs);
        if (leased.isEmpty()) {
            ex.sendResponseHeaders(204, -1);
            return;
        }
        send(ex, 200, view(leased.get(), true));
    }

    private void heartbeat(HttpExchange ex, String id) throws IOException {
        JsonNode b = body(ex);
        String token = requireText(b, "leaseToken");
        long leaseMs = clamp(b.path("leaseMs").asLong(defaultLeaseMillis), 1_000, 900_000);
        if (store.heartbeat(id, token, leaseMs)) {
            send(ex, 200, Map.of("ok", true));
        } else {
            leaseLost(ex);
        }
    }

    private void ack(HttpExchange ex, String id) throws IOException {
        String token = requireText(body(ex), "leaseToken");
        if (store.ack(id, token)) {
            send(ex, 200, Map.of("ok", true));
        } else {
            leaseLost(ex);
        }
    }

    private void fail(HttpExchange ex, String id) throws IOException {
        JsonNode b = body(ex);
        String token = requireText(b, "leaseToken");
        Optional<String> next = store.fail(id, token, b.path("error").asText("unknown error"));
        if (next.isPresent()) {
            send(ex, 200, Map.of("ok", true, "status", next.get()));
        } else {
            leaseLost(ex);
        }
    }

    private void getJob(HttpExchange ex, String id) throws IOException {
        Optional<Job> job = store.get(id);
        if (job.isEmpty()) {
            throw new ApiException(404, "not_found", "no such job");
        }
        send(ex, 200, view(job.get(), false));
    }

    private void dlq(HttpExchange ex) throws IOException {
        ArrayNode out = JSON.createArrayNode();
        for (Job j : store.listDead(100)) {
            out.add(view(j, false));
        }
        send(ex, 200, out);
    }

    private static void leaseLost(HttpExchange ex) throws IOException {
        send(ex, 409, Map.of("error", "lease_lost", "message", "this lease is no longer held by the caller"));
    }

    // ---- helpers ----

    /** The lease token is a bearer secret: it is only returned to the worker that just claimed the job. */
    private static ObjectNode view(Job j, boolean includeToken) throws IOException {
        ObjectNode n = JSON.createObjectNode();
        n.put("id", j.id());
        n.put("type", j.type());
        n.set("payload", JSON.readTree(j.payload()));
        n.put("status", j.status());
        n.put("runAt", j.runAt());
        n.put("priority", j.priority());
        n.put("attempts", j.attempts());
        n.put("maxAttempts", j.maxAttempts());
        n.put("leaseUntil", j.leaseUntil());
        n.put("createdAt", j.createdAt());
        if (j.lastError() != null) {
            n.put("lastError", j.lastError());
        }
        if (includeToken && j.leaseToken() != null) {
            n.put("leaseToken", j.leaseToken());
        }
        return n;
    }

    private static JsonNode body(HttpExchange ex) throws IOException {
        byte[] raw = ex.getRequestBody().readNBytes(MAX_BODY_BYTES + 1);
        if (raw.length > MAX_BODY_BYTES) {
            throw new ApiException(413, "too_large", "request body exceeds " + MAX_BODY_BYTES + " bytes");
        }
        return raw.length == 0 ? JSON.createObjectNode() : JSON.readTree(raw);
    }

    private static String requireText(JsonNode b, String field) {
        JsonNode v = b.get(field);
        if (v == null || !v.isTextual() || v.asText().isBlank()) {
            throw new ApiException(400, "missing_field", field + " is required");
        }
        return v.asText();
    }

    private static long clamp(long v, long lo, long hi) {
        return Math.max(lo, Math.min(hi, v));
    }

    private static void send(HttpExchange ex, int status, Object body) throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(body);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static final class ApiException extends RuntimeException {
        final int status;
        final String code;

        ApiException(int status, String code, String message) {
            super(message);
            this.status = status;
            this.code = code;
        }
    }
}
