package dev.pulse.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;

/** Thin synchronous client for the Pulse HTTP API. Safe to share between threads. */
public final class PulseClient {

    /** A job the caller currently holds a lease on. */
    public record JobHandle(String id, String type, JsonNode payload, int attempts, int maxAttempts,
                            String leaseToken, long leaseUntil) {}

    /** Optional submit settings; null fields use server defaults. */
    public record SubmitOptions(String id, Long runAt, Integer maxAttempts, Integer priority) {
        public static final SubmitOptions NONE = new SubmitOptions(null, null, null, null);
    }

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ObjectMapper json = new ObjectMapper();
    private final String base;

    public PulseClient(String baseUrl) {
        this.base = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    /**
     * Submits a job and returns its id. Supply {@code SubmitOptions.id} to make retries safe: resubmitting an
     * existing id returns the original job instead of creating a duplicate.
     */
    public String submit(String type, Object payload, SubmitOptions options) throws IOException, InterruptedException {
        ObjectNode b = json.createObjectNode();
        b.put("type", type);
        b.set("payload", json.valueToTree(payload));
        if (options.id() != null) b.put("id", options.id());
        if (options.runAt() != null) b.put("runAt", options.runAt());
        if (options.maxAttempts() != null) b.put("maxAttempts", options.maxAttempts());
        if (options.priority() != null) b.put("priority", options.priority());

        HttpResponse<String> r = post("/v1/jobs", b);
        if (r.statusCode() != 200 && r.statusCode() != 201) {
            throw new IOException("submit failed: HTTP " + r.statusCode() + " " + r.body());
        }
        return json.readTree(r.body()).get("id").asText();
    }

    public String submit(String type, Object payload) throws IOException, InterruptedException {
        return submit(type, payload, SubmitOptions.NONE);
    }

    /** Claims one due job, or returns empty if none is ready. */
    public Optional<JobHandle> poll(String workerId, Collection<String> types, long leaseMillis)
            throws IOException, InterruptedException {
        ObjectNode b = json.createObjectNode();
        b.put("workerId", workerId);
        b.put("leaseMs", leaseMillis);
        var arr = b.putArray("types");
        types.forEach(arr::add);

        HttpResponse<String> r = post("/v1/jobs/poll", b);
        if (r.statusCode() == 204) {
            return Optional.empty();
        }
        if (r.statusCode() != 200) {
            throw new IOException("poll failed: HTTP " + r.statusCode() + " " + r.body());
        }
        JsonNode n = json.readTree(r.body());
        return Optional.of(new JobHandle(
                n.get("id").asText(), n.get("type").asText(), n.get("payload"),
                n.get("attempts").asInt(), n.get("maxAttempts").asInt(),
                n.get("leaseToken").asText(), n.get("leaseUntil").asLong()));
    }

    /** Returns false if the lease was lost (expired and reclaimed). The caller should stop working on the job. */
    public boolean heartbeat(String id, String leaseToken, long leaseMillis) throws IOException, InterruptedException {
        ObjectNode b = json.createObjectNode();
        b.put("leaseToken", leaseToken);
        b.put("leaseMs", leaseMillis);
        return leaseCall("/v1/jobs/" + id + "/heartbeat", b);
    }

    /** Returns false if the lease was lost; the job may run again elsewhere. */
    public boolean ack(String id, String leaseToken) throws IOException, InterruptedException {
        ObjectNode b = json.createObjectNode();
        b.put("leaseToken", leaseToken);
        return leaseCall("/v1/jobs/" + id + "/ack", b);
    }

    public boolean fail(String id, String leaseToken, String error) throws IOException, InterruptedException {
        ObjectNode b = json.createObjectNode();
        b.put("leaseToken", leaseToken);
        b.put("error", error);
        return leaseCall("/v1/jobs/" + id + "/fail", b);
    }

    private boolean leaseCall(String path, ObjectNode body) throws IOException, InterruptedException {
        HttpResponse<String> r = post(path, body);
        if (r.statusCode() == 200) return true;
        if (r.statusCode() == 409) return false;
        throw new IOException("POST " + path + " failed: HTTP " + r.statusCode() + " " + r.body());
    }

    private HttpResponse<String> post(String path, Object body) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(35))
                .header("Content-Type", "application/json")
                .POST(BodyPublishers.ofString(json.writeValueAsString(body)))
                .build();
        return http.send(req, BodyHandlers.ofString());
    }
}
