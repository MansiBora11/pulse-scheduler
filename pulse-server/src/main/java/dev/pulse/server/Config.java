package dev.pulse.server;

/** Server settings, read from environment variables so the same image runs locally and on AWS. */
public record Config(
        int port,
        String region,
        String dynamoEndpoint,   // null on real AWS, e.g. http://localhost:8000 for DynamoDB Local
        String table,
        long defaultLeaseMillis,
        long reaperIntervalMillis) {

    public static Config fromEnv() {
        return new Config(
                intEnv("PULSE_PORT", 8080),
                env("AWS_REGION", "us-east-1"),
                emptyToNull(System.getenv("DYNAMO_ENDPOINT")),
                env("PULSE_TABLE", "pulse_jobs"),
                longEnv("PULSE_LEASE_MS", 30_000),
                longEnv("PULSE_REAPER_INTERVAL_MS", 1_000));
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? fallback : v;
    }

    private static int intEnv(String key, int fallback) {
        return Integer.parseInt(env(key, Integer.toString(fallback)));
    }

    private static long longEnv(String key, long fallback) {
        return Long.parseLong(env(key, Long.toString(fallback)));
    }

    private static String emptyToNull(String v) {
        return (v == null || v.isBlank()) ? null : v;
    }
}
