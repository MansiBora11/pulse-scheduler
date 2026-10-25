package dev.pulse.server;

import java.util.concurrent.ThreadLocalRandom;

/** Exponential backoff with full jitter: delay is random in [0, min(cap, base * 2^(attempt-1))]. */
public final class Backoff {
    private Backoff() {}

    public static final long BASE_MILLIS = 1_000;
    public static final long CAP_MILLIS = 5 * 60_000;

    /** Upper bound of the delay for the given attempt number (1 = first failure). */
    public static long ceiling(int attempt) {
        int shift = Math.min(Math.max(attempt - 1, 0), 20);
        return Math.min(CAP_MILLIS, BASE_MILLIS << shift);
    }

    public static long delayMillis(int attempt) {
        return ThreadLocalRandom.current().nextLong(ceiling(attempt) + 1);
    }
}
