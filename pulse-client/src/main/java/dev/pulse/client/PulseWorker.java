package dev.pulse.client;

import dev.pulse.client.PulseClient.JobHandle;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs handlers for jobs it claims. While a handler runs, a background heartbeat keeps the lease alive; if the
 * process dies the heartbeat stops, the lease expires, and the server hands the job to another worker.
 * Delivery is at-least-once, so handlers must be idempotent.
 */
public final class PulseWorker implements AutoCloseable {

    @FunctionalInterface
    public interface Handler {
        void handle(JobHandle job) throws Exception;
    }

    private static final System.Logger LOG = System.getLogger(PulseWorker.class.getName());
    private static final long IDLE_SLEEP_MILLIS = 200;
    private static final long ERROR_SLEEP_MILLIS = 1_000;

    private final PulseClient client;
    private final String workerId;
    private final long leaseMillis;
    private final Map<String, Handler> handlers = new ConcurrentHashMap<>();
    private final ExecutorService loops = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeats = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "pulse-heartbeat");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean running;

    public PulseWorker(PulseClient client, String workerId, long leaseMillis) {
        this.client = client;
        this.workerId = workerId;
        this.leaseMillis = leaseMillis;
    }

    public PulseWorker on(String type, Handler handler) {
        handlers.put(type, handler);
        return this;
    }

    /** Starts {@code concurrency} polling loops. Register handlers first. */
    public void start(int concurrency) {
        running = true;
        for (int i = 0; i < concurrency; i++) {
            loops.submit(this::loop);
        }
    }

    private void loop() {
        while (running) {
            try {
                Optional<JobHandle> job = client.poll(workerId, handlers.keySet(), leaseMillis);
                if (job.isEmpty()) {
                    if (!pause(IDLE_SLEEP_MILLIS)) return;
                    continue;
                }
                execute(job.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING, "poll loop error: {0}", e.toString());
                if (!pause(ERROR_SLEEP_MILLIS)) return;
            }
        }
    }

    private void execute(JobHandle job) {
        Handler handler = handlers.get(job.type());
        if (handler == null) {
            reportFailure(job, "no handler registered for type " + job.type());
            return;
        }
        AtomicBoolean leaseLost = new AtomicBoolean();
        long period = Math.max(500, leaseMillis / 3);

        ScheduledFuture<?> heartbeat = heartbeats.scheduleAtFixedRate(() -> {
            try {
                if (!client.heartbeat(job.id(), job.leaseToken(), leaseMillis)) {
                    leaseLost.set(true);
                }
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING, "heartbeat error for {0}: {1}", job.id(), e.toString());
            }
        }, period, period, TimeUnit.MILLISECONDS);

        try {
            handler.handle(job);
        } catch (Exception e) {
            heartbeat.cancel(false);
            if (e instanceof InterruptedException) {
                // Shutting down mid-job: let the lease expire so another worker retries it.
                Thread.currentThread().interrupt();
                return;
            }
            reportFailure(job, String.valueOf(e));
            return;
        }
        heartbeat.cancel(false);

        // The handler succeeded. A problem acknowledging it is not a job failure: the lease simply expires and
        // the job runs again, which at-least-once delivery allows.
        try {
            if (leaseLost.get()) {
                LOG.log(System.Logger.Level.WARNING, "lease lost for {0}; skipping ack", job.id());
            } else if (!client.ack(job.id(), job.leaseToken())) {
                LOG.log(System.Logger.Level.WARNING, "ack rejected for {0}; job may run again", job.id());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING, "ack failed for {0}: {1}; job will be retried after lease expiry",
                    job.id(), e.toString());
        }
    }

    private void reportFailure(JobHandle job, String reason) {
        try {
            client.fail(job.id(), job.leaseToken(), reason);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING, "could not report failure for {0}: {1}", job.id(), e.toString());
        }
    }

    private static boolean pause(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void close() {
        running = false;
        loops.shutdownNow();
        heartbeats.shutdownNow();
    }
}
