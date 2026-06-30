package net.teppan.backbone;

import net.teppan.shazo.Repository;
import net.teppan.shazo.ShazoException;
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
 *   <li>transaction-scoped {@linkplain #repository(Class) repositories} —
 *       all sharing one connection, so multi-entity work commits atomically;</li>
 *   <li>the authenticated {@link Principal}, optional {@code tenant}, and
 *       request {@link Locale};</li>
 *   <li>{@link #publish(Object...)} for domain events delivered <em>after</em> the
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

    // ── Transaction-scoped persistence (via the describer registry) ─────────────

    /**
     * Returns a repository for the registered domain {@code type}, bound to this
     * context's transaction. All repositories obtained from one context share a
     * single connection and commit together.
     *
     * <p>The service names only the domain type; which describer (and therefore
     * which storage) backs it is configured once on the runner via
     * {@link ServiceRunner.Builder#describers(net.teppan.shazo.jdbc.Repositories)}.
     * This keeps storage choice out of service signatures.
     *
     * @param type the domain type; never {@code null}
     * @param <T>  the domain type
     * @return a transaction-scoped repository
     * @throws IllegalStateException    if no describer registry is configured
     * @throws IllegalArgumentException if the type is not registered
     */
    public <T> Repository<T> repository(Class<T> type) {
        return requireDescribers().in(unitOfWork).repository(type);
    }

    // ── Store-anything facade (also via the describer registry) ─────────────────

    /**
     * Stores each entity in this transaction, dispatching by its runtime class —
     * the "store anything" convenience, for several types at once. Requires the
     * runner to have been built with
     * {@link ServiceRunner.Builder#describers(net.teppan.shazo.jdbc.Repositories)}.
     *
     * @param entities the entities to store; none {@code null}
     * @throws IllegalStateException    if no describer registry is configured
     * @throws IllegalArgumentException if an entity's type is not registered
     * @throws ShazoException           if a store fails
     */
    public void store(Object... entities) throws ShazoException {
        requireDescribers().in(unitOfWork).store(entities);
    }

    /**
     * Deletes each entity in this transaction, dispatching by its runtime class.
     *
     * @param entities the entities to delete; none {@code null}
     * @throws IllegalStateException    if no describer registry is configured
     * @throws IllegalArgumentException if an entity's type is not registered
     * @throws ShazoException           if a delete fails
     */
    public void delete(Object... entities) throws ShazoException {
        requireDescribers().in(unitOfWork).delete(entities);
    }

    /**
     * Returns whether the given entity exists, dispatching by its runtime class.
     *
     * @param entity the query entity; never {@code null}
     * @return {@code true} if a matching record exists
     * @throws IllegalStateException if no describer registry is configured
     * @throws ShazoException        if the check fails
     */
    public boolean contains(Object entity) throws ShazoException {
        return requireDescribers().in(unitOfWork).contains(entity);
    }

    /**
     * Retrieves an entity of the given type within this transaction.
     *
     * @param type  the entity type; never {@code null}
     * @param query a query entity carrying the key; never {@code null}
     * @param <T>   the entity type
     * @return the entity if found
     * @throws IllegalStateException if no describer registry is configured
     * @throws ShazoException        if the read fails
     */
    public <T> Optional<T> retrieve(Class<T> type, T query) throws ShazoException {
        return requireDescribers().in(unitOfWork).retrieve(type, query);
    }

    /**
     * Finds the unique entity of the given type within this transaction, throwing
     * if absent or ambiguous.
     *
     * @param type  the entity type; never {@code null}
     * @param query a query entity carrying the key; never {@code null}
     * @param <T>   the entity type
     * @return the single matching entity
     * @throws IllegalStateException if no describer registry is configured
     * @throws ShazoException        if absent, ambiguous, or the read fails
     */
    public <T> T find(Class<T> type, T query) throws ShazoException {
        return requireDescribers().in(unitOfWork).find(type, query);
    }

    /**
     * Fetches all rows of the given type as a raw table within this transaction,
     * without mapping to domain objects — for tabular consumers (UI grids,
     * reports, exports).
     *
     * @param type  the entity type; never {@code null}
     * @param query a query entity; never {@code null}
     * @param <T>   the entity type
     * @return the matching rows in table form
     * @throws IllegalStateException if no describer registry is configured
     * @throws ShazoException        if the read fails
     */
    public <T> net.teppan.shazo.RawResult catalog(Class<T> type, T query) throws ShazoException {
        return requireDescribers().in(unitOfWork).catalog(type, query);
    }

    /**
     * Gathers all entities of the given type into a list within this transaction.
     *
     * @param type  the entity type; never {@code null}
     * @param query a query entity; never {@code null}
     * @param <T>   the entity type
     * @return the matching entities
     * @throws IllegalStateException if no describer registry is configured
     * @throws ShazoException        if the read fails
     */
    public <T> List<T> gather(Class<T> type, T query) throws ShazoException {
        return requireDescribers().in(unitOfWork).gather(type, query);
    }

    private net.teppan.shazo.jdbc.Repositories requireDescribers() {
        var registry = runner.describers();
        if (registry == null) {
            throw new IllegalStateException(
                "No describer registry configured: build the ServiceRunner with "
                + ".describers(...) to use store/retrieve by type, or use repository(describer).");
        }
        return registry;
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
     * Publishes one or more domain events to be delivered to subscribers
     * <em>after</em> the transaction commits, in the order given. If the service
     * fails, the events are discarded along with the rolled-back transaction.
     *
     * @param events the events to publish; none {@code null}
     */
    public void publish(Object... events) {
        for (Object event : events) {
            pendingEvents.add(Objects.requireNonNull(event, "event"));
        }
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
