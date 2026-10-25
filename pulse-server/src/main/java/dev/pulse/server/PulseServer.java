package dev.pulse.server;

import com.sun.net.httpserver.HttpServer;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

public final class PulseServer {

    private static final System.Logger LOG = System.getLogger(PulseServer.class.getName());

    private PulseServer() {}

    public static void main(String[] args) throws Exception {
        Config cfg = Config.fromEnv();

        DynamoDbClientBuilder builder = DynamoDbClient.builder().region(Region.of(cfg.region()));
        if (cfg.dynamoEndpoint() != null) {
            builder.endpointOverride(URI.create(cfg.dynamoEndpoint()));
        }
        DynamoDbClient ddb = builder.build();

        JobStore store = new DynamoJobStore(ddb, cfg.table());
        store.ensureTable();

        ScheduledExecutorService reaper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "lease-reaper");
            t.setDaemon(true);
            return t;
        });
        reaper.scheduleWithFixedDelay(new LeaseReaper(store),
                cfg.reaperIntervalMillis(), cfg.reaperIntervalMillis(), TimeUnit.MILLISECONDS);

        HttpServer http = new HttpApi(store, cfg.defaultLeaseMillis()).start(cfg.port());
        LOG.log(System.Logger.Level.INFO, "Pulse listening on :{0} (table={1}, dynamo={2})",
                String.valueOf(cfg.port()), cfg.table(),
                cfg.dynamoEndpoint() == null ? "aws:" + cfg.region() : cfg.dynamoEndpoint());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            http.stop(2);
            reaper.shutdownNow();
            ddb.close();
        }, "shutdown"));
    }
}
