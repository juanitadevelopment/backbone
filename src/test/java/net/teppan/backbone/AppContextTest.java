package net.teppan.backbone;

import net.teppan.shazo.Describer;
import net.teppan.shazo.jdbc.SqlCommand;
import net.teppan.shazo.jdbc.embedded.EmbeddedDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link AppContext}, exercised through a {@link ServiceRunner} since
 * a context is always created around a unit of work.
 */
class AppContextTest {

    record Item(String id, String name) {}

    private DataSource ds;
    private Describer<Item, SqlCommand> items;

    @BeforeEach
    void setUp() throws Exception {
        ds = EmbeddedDataSource.inMemory("appctx_" + System.nanoTime());
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
            .infuser(r  -> r.first().map(row -> new Item(
                (String) row.get("id"), (String) row.get("name"))).orElseThrow())
            .cataloger(r -> r.rows().stream().map(row -> new Item(
                (String) row.get("id"), (String) row.get("name"))).toList())
            .build();
    }

    @Test
    void exposesPrincipalAndLocale() throws AppServiceException {
        var runner = ServiceRunner.builder().dataSource(ds).defaultLocale(Locale.JAPAN).build();
        var principal = new Principal("u1", "Alice", java.util.Set.of("ADMIN"));

        runner.run(ctx -> {
            assertThat(ctx.principal()).isEqualTo(principal);
            assertThat(ctx.locale()).isEqualTo(Locale.JAPAN);
            return null;
        }, principal);
    }

    @Test
    void tenant_emptyForSingleTenant() throws AppServiceException {
        var runner = ServiceRunner.builder().dataSource(ds).build();
        runner.run(ctx -> {
            assertThat(ctx.tenant()).isEqualTo(Optional.empty());
            return null;
        }, Principal.system());
    }

    @Test
    void tenant_presentWhenRouted() throws AppServiceException {
        var runner = ServiceRunner.builder().tenantRouter(t -> ds).build();
        runner.run(ctx -> {
            assertThat(ctx.tenant()).contains("acme");
            return null;
        }, Principal.system(), "acme", Locale.getDefault());
    }

    @Test
    void repository_roundTripWithinTransaction() throws AppServiceException {
        var registry = net.teppan.shazo.jdbc.Repositories.builder()
            .register(Item.class, items).build();
        var runner = ServiceRunner.builder().dataSource(ds).describers(registry).build();
        var found = runner.run(ctx -> {
            var repo = ctx.repository(Item.class);
            repo.store(new Item("k", "Kappa"));
            return repo.retrieve(new Item("k", null)).orElseThrow();
        }, Principal.system());
        assertThat(found.name()).isEqualTo("Kappa");
    }

    @Test
    void storeFacade_storesMultipleTypesByRuntimeClass() throws Exception {
        record Gizmo(String id, String label) {}
        try (var conn = ds.getConnection(); var st = conn.createStatement()) {
            st.execute("CREATE TABLE gizmo (id VARCHAR(36) PRIMARY KEY, label VARCHAR(200))");
        }
        var gizmos = net.teppan.shazo.Describer.<Gizmo, SqlCommand>builder()
            .contains(g -> List.of(SqlCommand.of("SELECT 1 FROM gizmo WHERE id = ?", g.id())))
            .store(g    -> List.of(SqlCommand.of(
                "MERGE INTO gizmo (id, label) KEY (id) VALUES (?, ?)", g.id(), g.label())))
            .delete(g   -> List.of(SqlCommand.of("DELETE FROM gizmo WHERE id = ?", g.id())))
            .retrieve(g -> List.of(SqlCommand.of("SELECT id, label FROM gizmo WHERE id = ?", g.id())))
            .catalog(g  -> List.of(SqlCommand.of("SELECT id, label FROM gizmo")))
            .infuser(r  -> r.first().map(row -> new Gizmo(
                (String) row.get("id"), (String) row.get("label"))).orElseThrow())
            .cataloger(r -> r.rows().stream().map(row -> new Gizmo(
                (String) row.get("id"), (String) row.get("label"))).toList())
            .build();

        var registry = net.teppan.shazo.jdbc.Repositories.builder()
            .register(Item.class, items)
            .register(Gizmo.class, gizmos)
            .build();
        var runner = ServiceRunner.builder().dataSource(ds).describers(registry).build();

        var found = runner.run(ctx -> {
            ctx.store(new Item("i1", "Item One"), new Gizmo("z1", "Gizmo One")); // varargs, two types
            return ctx.retrieve(Gizmo.class, new Gizmo("z1", null)).orElseThrow();
        }, Principal.system());
        assertThat(found.label()).isEqualTo("Gizmo One");

        // Both committed atomically.
        assertThat(new net.teppan.shazo.jdbc.JdbcRepository<>(ds, items)
            .contains(new Item("i1", null))).isTrue();
    }

    @Test
    void storeFacade_withoutRegistry_throws() {
        var runner = ServiceRunner.builder().dataSource(ds).build();
        // The service's IllegalStateException is wrapped by the runner, as any
        // non-AppServiceException is.
        assertThatThrownBy(() -> runner.run(ctx -> { ctx.store(new Item("x", "X")); return null; },
            Principal.system()))
            .isInstanceOf(AppServiceException.class)
            .hasRootCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void connection_isAvailableForRawSql() throws AppServiceException {
        var runner = ServiceRunner.builder().dataSource(ds).build();
        var ok = runner.run(ctx -> {
            try (var ps = ctx.connection().prepareStatement("SELECT 1")) {
                return ps.executeQuery().next();
            }
        }, Principal.system());
        assertThat(ok).isTrue();
    }

    @Test
    void connection_cannotBreakTheServiceTransaction() throws AppServiceException {
        var runner = ServiceRunner.builder().dataSource(ds).build();
        runner.run(ctx -> {
            var conn = ctx.connection();
            // The ServiceRunner owns the transaction boundary: business code must
            // not commit/roll back/close/detach the connection it borrows.
            assertThatThrownBy(conn::commit).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(conn::rollback).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(conn::close).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> conn.setAutoCommit(true))
                .isInstanceOf(UnsupportedOperationException.class);
            return null;
        }, Principal.system());
    }
}
