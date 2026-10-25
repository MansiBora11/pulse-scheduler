package dev.pulse.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongSupplier;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

/**
 * DynamoDB-backed job store.
 *
 * <p>Correctness rests on one rule: the status index is only used to <em>find candidates</em> (it is eventually
 * consistent). Every state change is a conditional write on the base table, so a stale read can waste work but
 * can never cause two workers to hold the same lease.
 *
 * <p>Table: partition key {@code jobId}. Index {@code status-dueAt-index}: partition key {@code status},
 * sort key {@code dueAt}. DONE jobs have no {@code dueAt} and therefore leave the (sparse) index.
 */
public final class DynamoJobStore implements JobStore {

    static final String GSI = "status-dueAt-index";
    private static final int CANDIDATES = 25;

    private final DynamoDbClient ddb;
    private final String table;
    private final LongSupplier clock;

    public DynamoJobStore(DynamoDbClient ddb, String table) {
        this(ddb, table, System::currentTimeMillis);
    }

    DynamoJobStore(DynamoDbClient ddb, String table, LongSupplier clock) {
        this.ddb = ddb;
        this.table = table;
        this.clock = clock;
    }

    @Override
    public void ensureTable() {
        try {
            ddb.describeTable(DescribeTableRequest.builder().tableName(table).build());
            return;
        } catch (ResourceNotFoundException e) {
            // fall through and create it
        }
        try {
            ddb.createTable(CreateTableRequest.builder()
                    .tableName(table)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(
                            attrDef("jobId", ScalarAttributeType.S),
                            attrDef("status", ScalarAttributeType.S),
                            attrDef("dueAt", ScalarAttributeType.N))
                    .keySchema(keyElem("jobId", KeyType.HASH))
                    .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                            .indexName(GSI)
                            .keySchema(keyElem("status", KeyType.HASH), keyElem("dueAt", KeyType.RANGE))
                            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                            .build())
                    .build());
        } catch (ResourceInUseException e) {
            // another instance created it first
        }
        ddb.waiter().waitUntilTableExists(DescribeTableRequest.builder().tableName(table).build());
    }

    @Override
    public SubmitResult submit(Job job) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("jobId", s(job.id()));
        item.put("type", s(job.type()));
        item.put("payload", s(job.payload()));
        item.put("status", s(Job.READY));
        item.put("runAt", n(job.runAt()));
        item.put("dueAt", n(job.runAt()));
        item.put("priority", n(job.priority()));
        item.put("attempts", n(0));
        item.put("maxAttempts", n(job.maxAttempts()));
        item.put("createdAt", n(job.createdAt()));
        try {
            ddb.putItem(PutItemRequest.builder()
                    .tableName(table)
                    .item(item)
                    .conditionExpression("attribute_not_exists(jobId)")
                    .build());
            return new SubmitResult(fromItem(item), true);
        } catch (ConditionalCheckFailedException e) {
            return new SubmitResult(get(job.id()).orElseThrow(), false);
        }
    }

    @Override
    public Optional<Job> get(String id) {
        Map<String, AttributeValue> item = ddb.getItem(GetItemRequest.builder()
                .tableName(table).key(key(id)).consistentRead(true).build()).item();
        return (item == null || item.isEmpty()) ? Optional.empty() : Optional.of(fromItem(item));
    }

    @Override
    public Optional<Job> lease(String workerId, Set<String> types, long leaseMillis) {
        long now = clock.getAsLong();
        List<Map<String, AttributeValue>> candidates = new ArrayList<>(queryByStatus(Job.READY, now, CANDIDATES));
        // Workers polling together would all race for the earliest item. Shuffling the window trades strict
        // dueAt order for far fewer wasted conditional writes. See docs/DESIGN.md.
        Collections.shuffle(candidates);

        for (Map<String, AttributeValue> candidate : candidates) {
            if (!types.isEmpty() && !types.contains(str(candidate, "type"))) {
                continue;
            }
            String token = UUID.randomUUID().toString();
            long until = now + leaseMillis;
            try {
                UpdateItemResponse r = ddb.updateItem(UpdateItemRequest.builder()
                        .tableName(table)
                        .key(Map.of("jobId", candidate.get("jobId")))
                        .conditionExpression("#s = :ready AND #d <= :now")
                        .updateExpression("SET #s = :leased, leaseToken = :tok, leaseUntil = :until, "
                                + "#d = :until, workerId = :w ADD attempts :one")
                        .expressionAttributeNames(Map.of("#s", "status", "#d", "dueAt"))
                        .expressionAttributeValues(Map.of(
                                ":ready", s(Job.READY),
                                ":leased", s(Job.LEASED),
                                ":now", n(now),
                                ":tok", s(token),
                                ":until", n(until),
                                ":w", s(workerId),
                                ":one", n(1)))
                        .returnValues(ReturnValue.ALL_NEW)
                        .build());
                return Optional.of(fromItem(r.attributes()));
            } catch (ConditionalCheckFailedException lostRace) {
                // another worker claimed it (or the index was stale); try the next candidate
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean heartbeat(String id, String leaseToken, long leaseMillis) {
        long until = clock.getAsLong() + leaseMillis;
        try {
            ddb.updateItem(UpdateItemRequest.builder()
                    .tableName(table)
                    .key(key(id))
                    .conditionExpression("#s = :leased AND leaseToken = :tok")
                    .updateExpression("SET leaseUntil = :until, #d = :until")
                    .expressionAttributeNames(Map.of("#s", "status", "#d", "dueAt"))
                    .expressionAttributeValues(Map.of(
                            ":leased", s(Job.LEASED),
                            ":tok", s(leaseToken),
                            ":until", n(until)))
                    .build());
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    @Override
    public boolean ack(String id, String leaseToken) {
        try {
            ddb.updateItem(UpdateItemRequest.builder()
                    .tableName(table)
                    .key(key(id))
                    .conditionExpression("#s = :leased AND leaseToken = :tok")
                    .updateExpression("SET #s = :done, finishedAt = :now REMOVE #d")
                    .expressionAttributeNames(Map.of("#s", "status", "#d", "dueAt"))
                    .expressionAttributeValues(Map.of(
                            ":leased", s(Job.LEASED),
                            ":tok", s(leaseToken),
                            ":done", s(Job.DONE),
                            ":now", n(clock.getAsLong())))
                    .build());
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    @Override
    public Optional<String> fail(String id, String leaseToken, String error) {
        return failLeased(id, leaseToken, error, false);
    }

    @Override
    public int reapExpired(int batchSize) {
        long now = clock.getAsLong();
        int reclaimed = 0;
        for (Map<String, AttributeValue> item : queryByStatus(Job.LEASED, now, batchSize)) {
            // The index can be stale: a heartbeat may have just extended this lease. failLeased re-checks
            // leaseUntil against the base table, so a renewed lease is never reclaimed.
            if (failLeased(str(item, "jobId"), str(item, "leaseToken"), "lease expired", true).isPresent()) {
                reclaimed++;
            }
        }
        return reclaimed;
    }

    @Override
    public List<Job> listDead(int limit) {
        List<Job> out = new ArrayList<>();
        for (Map<String, AttributeValue> item : ddb.query(QueryRequest.builder()
                .tableName(table)
                .indexName(GSI)
                .keyConditionExpression("#s = :st")
                .expressionAttributeNames(Map.of("#s", "status"))
                .expressionAttributeValues(Map.of(":st", s(Job.DEAD)))
                .limit(limit)
                .build()).items()) {
            out.add(fromItem(item));
        }
        return out;
    }

    /**
     * Moves a LEASED job to READY (with backoff) or DEAD (attempts exhausted). Conditional on the exact lease
     * token, so only the current holder, or the reaper after expiry, can do it.
     */
    private Optional<String> failLeased(String id, String leaseToken, String error, boolean onlyIfExpired) {
        Map<String, AttributeValue> item = ddb.getItem(GetItemRequest.builder()
                .tableName(table).key(key(id)).consistentRead(true).build()).item();
        if (item == null || item.isEmpty()
                || !Job.LEASED.equals(str(item, "status"))
                || leaseToken == null || !leaseToken.equals(str(item, "leaseToken"))) {
            return Optional.empty();
        }
        long now = clock.getAsLong();
        if (onlyIfExpired && num(item, "leaseUntil") > now) {
            return Optional.empty();
        }

        int attempts = (int) num(item, "attempts");
        int maxAttempts = (int) num(item, "maxAttempts");
        boolean dead = attempts >= maxAttempts;
        String next = dead ? Job.DEAD : Job.READY;
        long due = dead ? now : now + Backoff.delayMillis(attempts);

        String condition = "#s = :leased AND leaseToken = :tok";
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":leased", s(Job.LEASED));
        values.put(":tok", s(leaseToken));
        values.put(":next", s(next));
        values.put(":due", n(due));
        values.put(":err", s(truncate(error)));
        if (onlyIfExpired) {
            condition += " AND leaseUntil <= :now";
            values.put(":now", n(now));
        }

        try {
            ddb.updateItem(UpdateItemRequest.builder()
                    .tableName(table)
                    .key(key(id))
                    .conditionExpression(condition)
                    .updateExpression("SET #s = :next, #d = :due, lastError = :err REMOVE leaseToken, workerId")
                    .expressionAttributeNames(Map.of("#s", "status", "#d", "dueAt"))
                    .expressionAttributeValues(values)
                    .build());
            return Optional.of(next);
        } catch (ConditionalCheckFailedException e) {
            return Optional.empty();
        }
    }

    private List<Map<String, AttributeValue>> queryByStatus(String status, long dueBefore, int limit) {
        return ddb.query(QueryRequest.builder()
                .tableName(table)
                .indexName(GSI)
                .keyConditionExpression("#s = :st AND #d <= :now")
                .expressionAttributeNames(Map.of("#s", "status", "#d", "dueAt"))
                .expressionAttributeValues(Map.of(":st", s(status), ":now", n(dueBefore)))
                .limit(limit)
                .build()).items();
    }

    // ---- helpers ----

    private static Map<String, AttributeValue> key(String id) {
        return Map.of("jobId", s(id));
    }

    private static AttributeValue s(String v) {
        return AttributeValue.builder().s(v).build();
    }

    private static AttributeValue n(long v) {
        return AttributeValue.builder().n(Long.toString(v)).build();
    }

    private static String str(Map<String, AttributeValue> item, String name) {
        AttributeValue v = item.get(name);
        return v == null ? null : v.s();
    }

    private static long num(Map<String, AttributeValue> item, String name) {
        AttributeValue v = item.get(name);
        return (v == null || v.n() == null) ? 0 : Long.parseLong(v.n());
    }

    private static String truncate(String s) {
        if (s == null || s.isEmpty()) {
            return "unknown error";
        }
        return s.length() <= 1000 ? s : s.substring(0, 1000);
    }

    private static AttributeDefinition attrDef(String name, ScalarAttributeType type) {
        return AttributeDefinition.builder().attributeName(name).attributeType(type).build();
    }

    private static KeySchemaElement keyElem(String name, KeyType type) {
        return KeySchemaElement.builder().attributeName(name).keyType(type).build();
    }

    private static Job fromItem(Map<String, AttributeValue> i) {
        return new Job(
                str(i, "jobId"), str(i, "type"), str(i, "payload"), str(i, "status"),
                num(i, "runAt"), (int) num(i, "priority"), (int) num(i, "attempts"),
                (int) num(i, "maxAttempts"), str(i, "leaseToken"), str(i, "workerId"),
                num(i, "leaseUntil"), str(i, "lastError"), num(i, "createdAt"));
    }
}
