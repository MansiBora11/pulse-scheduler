package dev.pulse.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class BackoffTest {
    @Test
    void ceilingDoublesThenCaps() {
        assertEquals(1_000, Backoff.ceiling(1));
        assertEquals(2_000, Backoff.ceiling(2));
        assertEquals(4_000, Backoff.ceiling(3));
        assertEquals(Backoff.CAP_MILLIS, Backoff.ceiling(30));
    }

    @Test
    void delayStaysWithinCeiling() {
        for (int attempt = 1; attempt <= 12; attempt++) {
            for (int i = 0; i < 200; i++) {
                long d = Backoff.delayMillis(attempt);
                assertTrue(d >= 0 && d <= Backoff.ceiling(attempt));
            }
        }
    }
}
