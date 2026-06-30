package net.teppan.backbone;

import net.teppan.shazo.Describer;
import net.teppan.shazo.jdbc.Repositories;
import net.teppan.shazo.jdbc.SessionInitDataSource;
import net.teppan.shazo.jdbc.SqlCommand;
import net.teppan.shazo.jdbc.embedded.EmbeddedDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end multi-tenancy with schema-per-tenant on H2: one shared database,
 * one schema per tenant, each connection scoped with {@code SET SCHEMA} via
 * {@link SessionInitDataSource}. Exercises the {@code forTenant} handle, the
 * {@code withTenant} ambient scope, data isolation, and per-tenant durable events.
 */
class MultiTenantTest {

    record Item(String id, String name) {}
    record ItemAdded(String id) implements Serializable {}

    private Function<String, DataSource> router;
    private Repositories repos;

    @BeforeEach
    void setUp() throws Exception {
        var shared = EmbeddedDataSource.inMemory("mt_" + System.nanoTime());
        try (var conn = shared.getConnection(); var st = conn.createStatement()) {
            for (var t : List.of("acme", "globex")) {
                st.execute("CREATE SCHEMA IF NOT EXISTS " + t);
                st.execute("CREATE TABLE " + t + ".item (id VARCHAR(36) PRIMARY KEY, name VARCHAR(200))");
            }
        }
        // schema-per-tenant: scope each borrowed connection to the tenant's schema.
        router = tenant -> new SessionInitDataSource(shared, "SET SCHEMA " + tenant);

        var items = Describer.<Item, SqlCommand>builder()
            .contains(i -> List.of(SqlCommand.of("SELECT 1 FROM item WHERE id = ?", i.id())))
            .store(i    -> List.of(SqlCommand.of(
                "MERGE INTO item (id, name) KEY (id) VALUES (?, ?)", i.id(), i.name())))
            .delete(i   -> List.of(SqlCommand.of("DELETE FROM item WHERE id = ?", i.id())))
            .retrieve(i -> List.of(SqlCommand.of("SELECT id, name FROM item WHERE id = ?", i.id())))
            .catalog(i  -> List.of(SqlCommand.of("SELECT id, name FROM item")))
            .key(row    -> new Item((String) row.get("id"), null))
            .infuser(r  -> r.primary().first().map(row -> new Item(
                (String) row.get("id"), (String) row.get("name"))).orElseThrow())
            .build();
        repos = Repositories.builder().register(Item.class, items).build();
    }

    @Test
    void forTenant_isolatesDataByTenant() throws Exception {
        try (var runner = ServiceRunner.builder().tenantRouter(router).describers(repos).build()) {
            runner.forTenant("acme").run(ctx -> { ctx.store(new Item("1", "Acme-One")); return null; },
                Principal.system());
            runner.forTenant("globex").run(ctx -> { ctx.store(new Item("1", "Globex-One")); return null; },
                Principal.system());

            var acme = runner.forTenant("acme").run(
                ctx -> ctx.retrieve(Item.class, new Item("1", null)).orElseThrow(), Principal.system());
            var globex = runner.forTenant("globex").run(
                ctx -> ctx.retrieve(Item.class, new Item("1", null)).orElseThrow(), Principal.system());

            // Same id, different schema → fully isolated.
            assertThat(acme.name()).isEqualTo("Acme-One");
            assertThat(globex.name()).isEqualTo("Globex-One");
        }
    }

    @Test
    void withTenant_setsAmbientTenant() throws Exception {
        try (var runner = ServiceRunner.builder().tenantRouter(router).describers(repos).build()) {
            String seen = runner.withTenant("acme", () ->
                runner.run(ctx -> ctx.tenant().orElse("?"), Principal.system()));
            assertThat(seen).isEqualTo("acme");

            // Store via ambient tenant, read back via the handle.
            runner.withTenant("globex", () ->
                runner.run(ctx -> { ctx.store(new Item("x", "Via-Ambient")); return null; },
                    Principal.system()));
            var item = runner.forTenant("globex").run(
                ctx -> ctx.retrieve(Item.class, new Item("x", null)).orElseThrow(), Principal.system());
            assertThat(item.name()).isEqualTo("Via-Ambient");
        }
    }

    @Test
    void durableEvents_areDeliveredPerTenant() throws Exception {
        var delivered = new CopyOnWriteArrayList<String>();
        try (var runner = ServiceRunner.builder().tenantRouter(router).describers(repos)
                .durableEvents(ItemAdded.class)
                .outboxPollInterval(Duration.ofMillis(30))
                .subscribe(ItemAdded.class, e -> delivered.add(e.id()))
                .build()) {

            runner.forTenant("acme").run(ctx -> {
                ctx.store(new Item("a", "A")); ctx.publish(new ItemAdded("a")); return null;
            }, Principal.system());
            runner.forTenant("globex").run(ctx -> {
                ctx.store(new Item("g", "G")); ctx.publish(new ItemAdded("g")); return null;
            }, Principal.system());

            // Each tenant has its own backbone_outbox (in its schema) and poller;
            // both events are delivered.
            awaitUntil(() -> delivered.contains("a") && delivered.contains("g"));
            assertThat(runner.forTenant("acme").pendingEventCount()).isPresent();
            assertThat(runner.forTenant("globex").pendingEventCount()).isPresent();
        }
    }

    private static void awaitUntil(java.util.function.BooleanSupplier cond) throws Exception {
        long deadline = System.currentTimeMillis() + 3000;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < deadline) Thread.sleep(20);
        assertThat(cond.getAsBoolean()).isTrue();
    }
}
