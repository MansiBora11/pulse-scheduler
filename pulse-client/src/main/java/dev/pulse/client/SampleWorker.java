package dev.pulse.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

/** Example worker with a few fake handlers. Run it, then submit jobs of these types. */
public final class SampleWorker {

    private SampleWorker() {}

    public static void main(String[] args) throws Exception {
        String url = env("PULSE_URL", "http://localhost:8080");
        int concurrency = Integer.parseInt(env("WORKER_CONCURRENCY", "4"));
        String workerId = env("WORKER_ID", "java-" + ProcessHandle.current().pid());

        PulseWorker worker = new PulseWorker(new PulseClient(url), workerId, 30_000)
                .on("echo", job -> System.out.println("echo " + job.id() + " " + job.payload()))
                .on("slow", job -> Thread.sleep(ThreadLocalRandom.current().nextLong(50, 500)))
                .on("flaky", job -> {
                    if (ThreadLocalRandom.current().nextInt(100) < 20) {
                        throw new IllegalStateException("simulated failure");
                    }
                });

        Runtime.getRuntime().addShutdownHook(new Thread(worker::close));
        worker.start(concurrency);
        System.out.println(workerId + " polling " + url + " with " + concurrency + " loops");
        new CountDownLatch(1).await();
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? fallback : v;
    }
}
