package dev.pulse.server;

/**
 * One job. State machine: READY -> LEASED -> DONE, LEASED -> READY (retry), LEASED -> DEAD (retries exhausted).
 * {@code dueAt} is the next time something must happen to the job: runAt while READY, leaseUntil while LEASED.
 * DONE jobs have no dueAt, so they drop out of the status index.
 */
public record Job(
        String id,
        String type,
        String payload,      // raw JSON text, opaque to the scheduler
        String status,
        long runAt,
        int priority,        // stored now, honoured from Week 3
        int attempts,
        int maxAttempts,
        String leaseToken,   // null unless LEASED
        String workerId,
        long leaseUntil,
        String lastError,
        long createdAt) {

    public static final String READY = "READY";
    public static final String LEASED = "LEASED";
    public static final String DONE = "DONE";
    public static final String DEAD = "DEAD";
}
