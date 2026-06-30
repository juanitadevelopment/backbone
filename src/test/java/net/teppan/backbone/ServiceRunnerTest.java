package net.teppan.backbone;

import net.teppan.shazo.Describer;
import net.teppan.shazo.jdbc.JdbcRepository;
import net.teppan.shazo.jdbc.Repositories;
import net.teppan.shazo.jdbc.SqlCommand;
import net.teppan.shazo.jdbc.embedded.EmbeddedDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ServiceRunnerTest {

    record Item(String id, String name) {}
    record ItemCreated(String id) implements Serializable {}

    private DataSource ds;
    private Describer<Item, SqlCommand> items;
    private Repositories repos;

    @BeforeEach
    void setUp() throws Exception {
        ds = EmbeddedDataSource.inMemory("runner_" + System.nanoTime());
        try (var conn = ds.getConnection(); var st = conn.createStatement()) {
            st.execute("CREATE TABLE item (id VARCHAR(36) PRIMARY KEY, name VARCHAR(200))");
        }
        items = Describer.<Item, SqlCommand>builder()
            .contains(i -> List.of(SqlCommand.of("SELECT 1 FROM item WHERE id = ?", i.id())))
            .store(i    -> List.of(SqlCommand.of(
                "MERGE INTO item (id, name) KEY (id) VALUES (?, ?)", i.id(), i.name())))
            .delete(i   -> List.of(SqlCommand.of("DELETE FROM item WHERE id = ?", i.id())))
            .retrieve(i -> List.of(SqlCommand.of("SELECT id, name FROM item WHERE id = ?", i.id())))
            .catalog(i  -> List.of(SqlCommand.of("SELECT id, name FROM item")))
            .infuser(r  -> r.primary().first().map(row -> new Item(
                (String) row.get("id"), (String) row.get("name"))).orElseThrow())
            .key(row -> new Item((String) row.get("id"), null))
            .build();
        repos = Repositories.builder().register(Item.class, items).build();
    }

    private boolean persisted(String id) throws Exception {
        return new JdbcRepository<>(ds, items).contains(new Item(id, null));
    }

    // ── Registration & dispatch ────────────────────────────────────────────────

    @Test
    void execute_runsRegisteredService() throws AppServiceException {
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos)
            .register("greet", ctx -> "Hello, " + ctx.principal().displayName())
            .build();
        String result = runner.execute("greet", new Principal("u1", "Alice", Set.of()));
        assertThat(result).isEqualTo("Hello, Alice");
    }

    @Test
    void execute_usesDefaultLocale() throws AppServiceException {
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos).defaultLocale(Locale.JAPAN)
            .register("locale", ctx -> ctx.locale().toLanguageTag())
            .build();
        assertThat(runner.<String>execute("locale", Principal.anonymous())).isEqualTo("ja-JP");
    }

    @Test
    void execute_withExplicitLocale_overridesDefault() throws AppServiceException {
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos).defaultLocale(Locale.JAPAN)
            .register("locale", ctx -> ctx.locale().toLanguageTag())
            .build();
        assertThat(runner.<String>execute("locale", Principal.anonymous(), null, Locale.US))
            .isEqualTo("en-US");
    }

    @Test
    void execute_unknownName_throwsIllegalArgumentException() {
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos).build();
        assertThatThrownBy(() -> runner.execute("no-such-service", Principal.anonymous()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("no-such-service");
    }

    @Test
    void serviceNames_listsRegisteredServices() {
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos)
            .register("a", ctx -> null)
            .register("b", ctx -> null)
            .build();
        assertThat(runner.serviceNames()).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void pendingEventCount_emptyWithoutOutbox() {
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos).build();
        assertThat(runner.pendingEventCount()).isEmpty();
    }

    @Test
    void pendingEventCount_presentWithOutbox() {
        try (var runner = ServiceRunner.builder().dataSource(ds).describers(repos)
                .durableEvents(ItemCreated.class).build()) {
            assertThat(runner.pendingEventCount()).isPresent();
            assertThat(runner.pendingEventCount().getAsLong()).isZero();
        }
    }

    @Test
    void outboxManagement_emptyWithoutOutbox() {
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos).build();
        assertThat(runner.deadLetterCount()).isEmpty();
        assertThat(runner.pendingEvents(10)).isEmpty();
        assertThat(runner.deadLetterEvents(10)).isEmpty();
        assertThat(runner.retryEvent(1)).isFalse();
        assertThat(runner.discardEvent(1)).isFalse();
    }

    @Test
    void deadLetterThenRetry_throughRunner() throws Exception {
        var down = new java.util.concurrent.atomic.AtomicBoolean(true);
        var delivered = new java.util.concurrent.CopyOnWriteArrayList<String>();
        try (var runner = ServiceRunner.builder().dataSource(ds).describers(repos)
                .durableEvents(ItemCreated.class)
                .outboxMaxAttempts(2)
                .outboxPollInterval(java.time.Duration.ofMillis(30))
                .subscribe(ItemCreated.class, e -> {
                    if (down.get()) throw new RuntimeException("subscriber down");
                    delivered.add(e.id());
                })
                .register("create", ctx -> {
                    ctx.repository(Item.class).store(new Item("x1", "X"));
                    ctx.publish(new ItemCreated("x1"));
                    return null;
                })
                .build()) {

            runner.execute("create", Principal.system());

            // The subscriber keeps failing, so the event is dead-lettered.
            awaitUntil(() -> runner.deadLetterCount().orElse(0) >= 1);
            assertThat(runner.pendingEventCount().getAsLong()).isZero();
            var dead = runner.deadLetterEvents(10);
            assertThat(dead).hasSize(1);
            assertThat(dead.get(0).type()).isEqualTo(ItemCreated.class.getName());

            // Fix the subscriber and requeue: it is now delivered.
            down.set(false);
            assertThat(runner.retryEvent(dead.get(0).id())).isTrue();
            awaitUntil(() -> !delivered.isEmpty());
            assertThat(delivered).containsExactly("x1");
            assertThat(runner.deadLetterCount().getAsLong()).isZero();
        }
    }

    private static void awaitUntil(java.util.function.BooleanSupplier cond) throws Exception {
        long deadline = System.currentTimeMillis() + 3000;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(cond.getAsBoolean()).isTrue();
    }

    @Test
    void run_adHocService_executesWithContext() throws AppServiceException {
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos).build();
        var result = runner.run(ctx -> ctx.principal().id(), Principal.system());
        assertThat(result).isEqualTo("system");
    }

    @Test
    void builder_withoutDataSource_throwsIllegalStateException() {
        assertThatThrownBy(() -> ServiceRunner.builder().build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("dataSource");
    }

    // ── Transactional behaviour ────────────────────────────────────────────────

    @Test
    void multipleWritesCommitAtomically() throws Exception {
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos).build();
        runner.run(ctx -> {
            var repo = ctx.repository(Item.class);
            repo.store(new Item("a", "Alpha"));
            repo.store(new Item("b", "Beta"));
            return null;
        }, Principal.system());

        assertThat(persisted("a")).isTrue();
        assertThat(persisted("b")).isTrue();
    }

    @Test
    void serviceFailure_rollsBackAllWrites() throws Exception {
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos).build();
        assertThatThrownBy(() -> runner.run(ctx -> {
            ctx.repository(Item.class).store(new Item("c", "Gamma"));
            throw new AppServiceException("boom");
        }, Principal.system())).isInstanceOf(AppServiceException.class);

        assertThat(persisted("c")).isFalse();
    }

    @Test
    void nonAppServiceException_wrappedInAppServiceException() {
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos).build();
        assertThatThrownBy(() -> runner.run(ctx -> {
            throw new RuntimeException("raw error");
        }, Principal.anonymous()))
            .isInstanceOf(AppServiceException.class)
            .hasRootCauseMessage("raw error");
    }

    // ── Post-commit events & actions ───────────────────────────────────────────

    @Test
    void publishedEvents_deliveredToSubscriberAfterCommit() throws Exception {
        List<String> delivered = new ArrayList<>();
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos)
            .subscribe(ItemCreated.class, e -> delivered.add(e.id()))
            .build();

        runner.run(ctx -> {
            ctx.repository(Item.class).store(new Item("x", "Xi"));
            ctx.publish(new ItemCreated("x"));
            return null;
        }, Principal.system());

        assertThat(delivered).containsExactly("x");
        assertThat(persisted("x")).isTrue();
    }

    @Test
    void publishedEvents_varargs_deliversAllInOrderAfterCommit() throws Exception {
        List<String> delivered = new ArrayList<>();
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos)
            .subscribe(ItemCreated.class, e -> delivered.add(e.id()))
            .build();

        runner.run(ctx -> {
            ctx.publish(new ItemCreated("a"), new ItemCreated("b"), new ItemCreated("c"));
            return null;
        }, Principal.system());

        assertThat(delivered).containsExactly("a", "b", "c");
    }

    @Test
    void publishedEvents_notDeliveredWhenServiceFails() {
        List<String> delivered = new ArrayList<>();
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos)
            .subscribe(ItemCreated.class, e -> delivered.add(e.id()))
            .build();

        assertThatThrownBy(() -> runner.run(ctx -> {
            ctx.publish(new ItemCreated("y"));
            throw new AppServiceException("nope");
        }, Principal.system())).isInstanceOf(AppServiceException.class);

        assertThat(delivered).isEmpty();
    }

    @Test
    void afterCommit_runsAfterSuccess() throws AppServiceException {
        List<String> log = new ArrayList<>();
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos).build();
        runner.run(ctx -> {
            ctx.afterCommit(() -> log.add("committed"));
            return null;
        }, Principal.anonymous());
        assertThat(log).containsExactly("committed");
    }

    @Test
    void afterCommit_doesNotRunWhenServiceThrows() {
        List<String> log = new ArrayList<>();
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos).build();
        assertThatThrownBy(() -> runner.run(ctx -> {
            ctx.afterCommit(() -> log.add("should-not-run"));
            throw new AppServiceException("boom");
        }, Principal.anonymous())).isInstanceOf(AppServiceException.class);
        assertThat(log).isEmpty();
    }

    @Test
    void postCommitFailures_collectedAsSuppressed() {
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos).build();
        assertThatThrownBy(() -> runner.run(ctx -> {
            ctx.afterCommit(() -> { throw new RuntimeException("first"); });
            ctx.afterCommit(() -> { throw new RuntimeException("second"); });
            return null;
        }, Principal.anonymous()))
            .isInstanceOf(AppServiceException.class)
            .satisfies(e -> assertThat(e.getSuppressed()).hasSize(2));
    }

    // ── Nested composition ─────────────────────────────────────────────────────

    @Test
    void nestedCall_sharesTransaction_rollbackUndoesInnerWrites() throws Exception {
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos).build();

        AppService<Void> inner = ctx -> {
            ctx.repository(Item.class).store(new Item("inner", "Inner"));
            return null;
        };

        assertThatThrownBy(() -> runner.run(ctx -> {
            ctx.repository(Item.class).store(new Item("outer", "Outer"));
            ctx.call(inner);                 // joins the same transaction
            throw new AppServiceException("rollback both");
        }, Principal.system())).isInstanceOf(AppServiceException.class);

        // Inner write rolled back with the outer transaction.
        assertThat(persisted("inner")).isFalse();
        assertThat(persisted("outer")).isFalse();
    }

    // ── Durable events (transactional outbox) ──────────────────────────────────

    @Test
    void durableEvents_persistedInTxAndDeliveredAfterCommit() throws Exception {
        var delivered = new CopyOnWriteArrayList<String>();
        var latch = new CountDownLatch(1);
        try (var runner = ServiceRunner.builder().dataSource(ds).describers(repos)
                .durableEvents(ItemCreated.class)
                .outboxPollInterval(java.time.Duration.ofMillis(50))
                .subscribe(ItemCreated.class, e -> { delivered.add(e.id()); latch.countDown(); })
                .build()) {

            runner.run(ctx -> {
                ctx.repository(Item.class).store(new Item("d1", "Durable"));
                ctx.publish(new ItemCreated("d1"));
                return null;
            }, Principal.system());

            assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(delivered).containsExactly("d1");
            assertThat(persisted("d1")).isTrue();
        }
    }

    @Test
    void durableEvents_notDeliveredWhenServiceFails() throws Exception {
        var delivered = new CopyOnWriteArrayList<String>();
        try (var runner = ServiceRunner.builder().dataSource(ds).describers(repos)
                .durableEvents(ItemCreated.class)
                .outboxPollInterval(java.time.Duration.ofMillis(50))
                .subscribe(ItemCreated.class, e -> delivered.add(e.id()))
                .build()) {

            assertThatThrownBy(() -> runner.run(ctx -> {
                ctx.publish(new ItemCreated("d2"));
                throw new AppServiceException("rollback");
            }, Principal.system())).isInstanceOf(AppServiceException.class);

            Thread.sleep(300);   // give the poller time to (not) deliver
            assertThat(delivered).isEmpty();
        }
    }

    @Test
    void nestedCall_eventsFlushedOnceAtOuterCommit() throws Exception {
        List<String> delivered = new ArrayList<>();
        var runner = ServiceRunner.builder().dataSource(ds).describers(repos)
            .subscribe(ItemCreated.class, e -> delivered.add(e.id()))
            .build();

        AppService<Void> inner = ctx -> {
            ctx.repository(Item.class).store(new Item("n1", "N1"));
            ctx.publish(new ItemCreated("n1"));
            return null;
        };

        runner.run(ctx -> {
            ctx.call(inner);
            ctx.publish(new ItemCreated("n0"));
            return null;
        }, Principal.system());

        assertThat(delivered).containsExactlyInAnyOrder("n1", "n0");
        assertThat(persisted("n1")).isTrue();
    }
}
