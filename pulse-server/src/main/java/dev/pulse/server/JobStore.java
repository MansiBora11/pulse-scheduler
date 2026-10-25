package dev.pulse.server;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Storage contract. Every state change must be atomic and conditional so concurrent callers cannot both win. */
public interface JobStore {

    record SubmitResult(Job job, boolean created) {}

    /** Creates the backing table and index if they do not exist. */
    void ensureTable();

    /** Idempotent on job id: resubmitting an existing id returns the stored job with created=false. */
    SubmitResult submit(Job job);

    Optional<Job> get(String id);

    /** Atomically claims one due READY job, or returns empty. Exactly one concurrent caller can claim a given job. */
    Optional<Job> lease(String workerId, Set<String> types, long leaseMillis);

    /** Extends the lease. False means the caller no longer holds it. */
    boolean heartbeat(String id, String leaseToken, long leaseMillis);

    /** Marks the job DONE. False means the caller no longer holds the lease. */
    boolean ack(String id, String leaseToken);

    /** Records a failure. Returns the new status (READY for retry, DEAD when exhausted) or empty if the lease was lost. */
    Optional<String> fail(String id, String leaseToken, String error);

    /** Returns expired leases to READY (or DEAD). Returns how many jobs were reclaimed. */
    int reapExpired(int batchSize);

    /** Dead-letter queue contents. */
    List<Job> listDead(int limit);
}
