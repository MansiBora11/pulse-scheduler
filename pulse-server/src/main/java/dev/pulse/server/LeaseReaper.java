package dev.pulse.server;

/** Periodically returns jobs whose lease expired (worker stopped sending heartbeats) to the queue. */
public final class LeaseReaper implements Runnable {

    private static final System.Logger LOG = System.getLogger(LeaseReaper.class.getName());
    private static final int BATCH = 100;
    private static final int MAX_PER_PASS = 10_000;

    private final JobStore store;

    public LeaseReaper(JobStore store) {
        this.store = store;
    }

    @Override
    public void run() {
        try {
            int total = 0;
            int reclaimed;
            do {
                reclaimed = store.reapExpired(BATCH);
                total += reclaimed;
            } while (reclaimed == BATCH && total < MAX_PER_PASS);
            if (total > 0) {
                LOG.log(System.Logger.Level.INFO, "reclaimed {0} expired lease(s)", total);
            }
        } catch (RuntimeException e) {
            // A scheduled task that throws is silently cancelled, so never let an error escape.
            LOG.log(System.Logger.Level.ERROR, "reaper pass failed", e);
        }
    }
}
