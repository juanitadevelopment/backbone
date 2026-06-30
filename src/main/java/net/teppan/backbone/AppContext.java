package net.teppan.backbone;

import net.teppan.shazo.Describer;
import net.teppan.shazo.Repository;
import net.teppan.shazo.jdbc.SqlCommand;
import net.teppan.shazo.jdbc.UnitOfWork;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Per-request transactional context passed to every {@link AppService}.
 *
 * <p>An {@code AppContext} is created by a {@link ServiceRunner} around a single
 * {@link UnitOfWork} (one database transaction). It exposes:
 *
 * <ul>
 *   <li>transaction-scoped {@linkplain #repository(Describer) repositories} —
 *       all sharing one connection, so multi-entity work commits atomically;</li>
 *   <li>the authenticated {@link Principal}, optional {@code tenant}, and
 *       request {@link Locale};</li>
 *   <li>{@link #publish(Object)} for domain events delivered <em>after</em> the
 *       transaction commits;</li>
 *   <li>{@link #afterCommit(Runnable)} for arbitrary post-commit work;</li>
 *   <li>{@link #call(AppService)} to invoke another service within the
 *       <em>same</em> transaction (nested composition).</li>
 * </ul>
 *
 * <p>Published events and after-commit actions are buffered and run by the
 * {@link ServiceRunner} only on successful commit; if the service throws they
 * are discarded along with the rolled-back transaction. An instance is confined
 * to the thread executing the service and is not thread-safe.
 *
 * @see ServiceRunner
 * @see AppService
 */
public final class AppContext {

    private final UnitOfWork unitOfWork;
    private final Principal principal;
    private final String tenant;          // nullable: single-tenant
    private final Locale locale;
    private final ServiceRunner runner;

    private final List<Object> pendingEvents = new ArrayList<>();
    private final List<Runnable> afterCommitActions = new ArrayList<>();

    AppContext(UnitOfWork unitOfWork, Principal principal, String tenant,
               Locale locale, ServiceRunner runner) {
        this.unitOfWork = Objects.requireNonNull(unitOfWork, "unitOfWork");
        this.principal  = Objects.requireNonNull(principal,  "principal");
        this.tenant     = tenant;
        this.locale     = Objects.requireNonNull(locale,     "locale");
        this.runner     = Objects.requireNonNull(runner,     "runner");
    }

    // ── Transaction-scoped persistence ─────────────────────────────────────────

    /**
     * Returns a repository for {@code describer} bound to this context's
     * transaction. All repositories obtained from one context share a single
     * connection and commit together.
     *
     * @param describer the describer for domain type {@code T}; never {@code null}
     * @param <T>       the domain type
     * @return a transaction-scoped repository
     */
    public <T> Repository<T> repository(Describer<T, SqlCommand> describer) {
        return unitOfWork.repository(describer);
    }

    /**
     * Returns the JDBC connection backing this transaction, for statements that
     * do not fit the repository model.
     *
     * <p>The returned connection is a guarded view whose transaction boundary the
     * {@link ServiceRunner} owns: {@code commit()}, {@code rollback()},
     * {@code close()}, {@code setAutoCommit(boolean)}, and {@code abort(Executor)}
     * throw {@link UnsupportedOperationException}. Every other operation is
     * forwarded to the real connection.
     *
     * @return the transaction's connection; never {@code null}
     */
    public Connection connection() {
        return unitOfWork.connection();
    }

    // ── Request attributes ─────────────────────────────────────────────────────

    /**
     * Returns the authenticated principal.
     *
     * @return the principal; never {@code null}
     */
    public Principal principal() {
        return principal;
    }

    /**
     * Returns the tenant this request is scoped to, if any.
     *
     * @return the tenant identifier, or empty in single-tenant deployments
     */
    public Optional<String> tenant() {
        return Optional.ofNullable(tenant);
    }

    /**
     * Returns the request locale.
     *
     * @return the locale; never {@code null}
     */
    public Locale locale() {
        return locale;
    }

    // ── Deferred (post-commit) work ────────────────────────────────────────────

    /**
     * Publishes a domain event to be delivered to subscribers <em>after</em> the
     * transaction commits. If the service fails, the event is discarded.
     *
     * @param event the event to publish; never {@code null}
     */
    public void publish(Object event) {
        pendingEvents.add(Objects.requireNonNull(event, "event"));
    }

    /**
     * Registers an action to run after the transaction commits, in registration
     * order. If the service fails, the action does not run.
     *
     * @param action the deferred action; never {@code null}
     */
    public void afterCommit(Runnable action) {
        afterCommitActions.add(Objects.requireNonNull(action, "action"));
    }

    // ── Nested composition ─────────────────────────────────────────────────────

    /**
     * Invokes another service within this same transaction (propagation: join).
     * The nested service shares this context — its repositories, published
     * events, and after-commit actions all participate in the current unit of
     * work and are committed/flushed together at the outer boundary.
     *
     * @param service the service to invoke; never {@code null}
     * @param <R>     the nested service's result type
     * @return the nested service result
     * @throws AppServiceException if the nested service fails
     */
    public <R> R call(AppService<R> service) throws AppServiceException {
        return runner.callNested(this, Objects.requireNonNull(service, "service"));
    }

    // ── Package-private: consumed by ServiceRunner after commit ────────────────

    List<Object> pendingEvents() {
        return pendingEvents;
    }

    List<Runnable> afterCommitActions() {
        return afterCommitActions;
    }
}
