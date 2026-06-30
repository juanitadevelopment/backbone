package net.teppan.backbone;

import net.teppan.backbone.event.Outbox;
import net.teppan.backbone.event.OutboxEntry;
import net.teppan.shazo.ShazoException;
import net.teppan.shazo.jdbc.Repositories;
import net.teppan.shazo.jdbc.Transactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Runs {@link AppService}s, each as one transactional unit of work, and delivers
 * the domain events they publish after the transaction commits.
 *
 * <p>For every {@link #execute} / {@link #run} call the runner opens a
 * transaction (via a {@link Transactor}), builds an {@link AppContext} around
 * it, invokes the service, and commits. Only on a successful commit are the
 * service's published events delivered and its {@linkplain
 * AppContext#afterCommit after-commit} actions run; a service that throws causes
 * a rollback and those deferred actions are discarded.
 *
 * <p>Nested services invoked through {@link AppContext#call(AppService)} join
 * the caller's transaction; their events and after-commit actions are flushed
 * with the outer call.
 *
 * <h2>Event delivery</h2>
 * <p>By default events are delivered <em>in-process</em>, synchronously, right
 * after commit. Enabling {@linkplain Builder#durableEvents(Class[]) durable
 * events} switches to a transactional {@link Outbox}: events are written in the
 * same transaction as the business change and delivered asynchronously,
 * at-least-once, surviving restarts. Either way they reach the same
 * {@linkplain Builder#subscribe subscribers}.
 *
 * <h2>Example</h2>
 * <pre>{@code
 * try (var runner = ServiceRunner.builder()
 *         .dataSource(dataSource)
 *         .durableEvents(OrderCreated.class)
 *         .subscribe(OrderCreated.class, e -> mailer.sendConfirmation(e.orderId()))
 *         .register("createOrder", ctx -> {
 *             ctx.repository(orderDescriber).store(order);
 *             ctx.publish(new OrderCreated(order.id()));
 *             return order.id();
 *         })
 *         .build()) {
 *     String orderId = runner.execute("createOrder", principal);
 * }
 * }</pre>
 *
 * <p>When durable events are enabled the runner owns a background poller and
 * must be {@linkplain #close() closed}.
 */
public final class ServiceRunner implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ServiceRunner.class);

    private final Function<String, DataSource> router;
    private final Locale defaultLocale;
    private final Map<String, AppService<?>> services;
    private final List<Subscription<?>> subscriptions;
    private final ConcurrentHashMap<String, Transactor> transactors = new ConcurrentHashMap<>();
    private final Repositories describers;  // null when no registry configured

    // Durable events: one Outbox per tenant (each on that tenant's DataSource),
    // created lazily on first use. Empty types = events delivered in-process.
    private final boolean durableEnabled;
    private final List<Class<?>> outboxTypes;
    private final Duration outboxPollInterval;
    private final Duration outboxRetention;
    private final int outboxMaxAttempts;
    private final ConcurrentHashMap<String, DataSource> dataSources = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Outbox> outboxes = new ConcurrentHashMap<>();

    // Ambient tenant for withTenant(...) scopes. ThreadLocal today; can become a
    // ScopedValue once the baseline moves to JDK 25 — the public API is unchanged.
    private final ThreadLocal<String> ambientTenant = new ThreadLocal<>();

    private record Subscription<E>(Class<E> type, Consumer<E> listener) {}

    private ServiceRunner(Function<String, DataSource> router, Locale defaultLocale,
                          Map<String, AppService<?>> services,
                          List<Subscription<?>> subscriptions,
                          List<Class<?>> outboxTypes,
                          Duration pollInterval, Duration retention, int maxAttempts,
                          Repositories describers) {
        this.router             = router;
        this.defaultLocale      = defaultLocale;
        this.services           = Map.copyOf(services);
        this.subscriptions      = List.copyOf(subscriptions);
        this.describers         = describers;
        this.durableEnabled     = !outboxTypes.isEmpty();
        this.outboxTypes        = List.copyOf(outboxTypes);
        this.outboxPollInterval = pollInterval;
        this.outboxRetention    = retention;
        this.outboxMaxAttempts  = maxAttempts;
    }

    /**
     * Returns a new builder.
     *
     * @return a builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    // ── Named service execution ───────────────────────────────────────────────

    /**
     * Executes a registered service by name using the default locale and no tenant.
     *
     * @param name      the service name; never {@code null}
     * @param principal the authenticated caller; never {@code null}
     * @param <R>       the service return type
     * @return the service result
     * @throws AppServiceException      if the service or a post-commit action fails
     * @throws IllegalArgumentException if {@code name} is not registered
     */
    public <R> R execute(String name, Principal principal) throws AppServiceException {
        return execute(name, principal, ambientTenant.get(), defaultLocale);
    }

    /**
     * Executes a registered service by name routed to the given tenant.
     *
     * @param name      the service name; never {@code null}
     * @param principal the authenticated caller; never {@code null}
     * @param tenant    the tenant to route to; may be {@code null}
     * @param <R>       the service return type
     * @return the service result
     * @throws AppServiceException      if the service or a post-commit action fails
     * @throws IllegalArgumentException if {@code name} is not registered
     */
    public <R> R execute(String name, Principal principal, String tenant)
            throws AppServiceException {
        return execute(name, principal, tenant, defaultLocale);
    }

    /**
     * Executes a registered service by name with an explicit tenant and locale.
     *
     * @param name      the service name; never {@code null}
     * @param principal the authenticated caller; never {@code null}
     * @param tenant    the tenant to route to; may be {@code null}
     * @param locale    the request locale; never {@code null}
     * @param <R>       the service return type
     * @return the service result
     * @throws AppServiceException      if the service or a post-commit action fails
     * @throws IllegalArgumentException if {@code name} is not registered
     */
    @SuppressWarnings("unchecked")
    public <R> R execute(String name, Principal principal, String tenant, Locale locale)
            throws AppServiceException {
        var service = (AppService<R>) services.get(Objects.requireNonNull(name, "name"));
        if (service == null) {
            throw new IllegalArgumentException("Unknown service: " + name);
        }
        return run(service, principal, tenant, locale);
    }

    // ── Ad-hoc service execution ──────────────────────────────────────────────

    /**
     * Runs an ad-hoc (non-registered) service using the default locale and no tenant.
     *
     * @param service   the service to execute; never {@code null}
     * @param principal the authenticated caller; never {@code null}
     * @param <R>       the service return type
     * @return the service result
     * @throws AppServiceException if the service or a post-commit action fails
     */
    public <R> R run(AppService<R> service, Principal principal) throws AppServiceException {
        return run(service, principal, ambientTenant.get(), defaultLocale);
    }

    /**
     * Runs an ad-hoc service with an explicit locale.
     *
     * @param service   the service to execute; never {@code null}
     * @param principal the authenticated caller; never {@code null}
     * @param locale    the request locale; never {@code null}
     * @param <R>       the service return type
     * @return the service result
     * @throws AppServiceException if the service or a post-commit action fails
     */
    public <R> R run(AppService<R> service, Principal principal, Locale locale)
            throws AppServiceException {
        return run(service, principal, null, locale);
    }

    /**
     * Runs an ad-hoc service with an explicit tenant and locale.
     *
     * @param service   the service to execute; never {@code null}
     * @param principal the authenticated caller; never {@code null}
     * @param tenant    the tenant to route to; may be {@code null}
     * @param locale    the request locale; never {@code null}
     * @param <R>       the service return type
     * @return the service result
     * @throws AppServiceException if the service or a post-commit action fails
     */
    public <R> R run(AppService<R> service, Principal principal, String tenant, Locale locale)
            throws AppServiceException {
        Objects.requireNonNull(service,   "service");
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(locale,    "locale");

        // Resolve this tenant's outbox up front (creates its table + poller once).
        var outbox = outboxFor(tenant);
        var ctxRef = new AtomicReference<AppContext>();
        R result;
        try {
            result = transactorFor(tenant).execute(uow -> {
                var ctx = new AppContext(uow, principal, tenant, locale, this);
                ctxRef.set(ctx);
                try {
                    R r = service.execute(ctx);
                    if (outbox != null) {
                        // Persist events in the same transaction (this tenant's DB).
                        outbox.write(uow.connection(), ctx.pendingEvents());
                    }
                    return r;
                } catch (AppServiceException e) {
                    throw new ServiceFailure(e);
                } catch (Exception e) {
                    throw new ServiceFailure(new AppServiceException("Service execution failed", e));
                }
            });
        } catch (ShazoException e) {
            if (e.getCause() instanceof ServiceFailure sf) {
                throw sf.appException;   // rolled back; deferred work discarded
            }
            throw new AppServiceException("Service transaction failed", e);
        }
        // Committed: deliver events (now durable if using the outbox) and run
        // after-commit actions.
        var ctx = ctxRef.get();
        if (outbox != null) {
            outbox.poke();
        }
        flushPostCommit(ctx, /* dispatchEventsInProcess = */ outbox == null);
        return result;
    }

    // ── Tenant binding ─────────────────────────────────────────────────────────

    /**
     * Returns a view of this runner bound to {@code tenant}: its {@code execute} /
     * {@code run} calls route to that tenant's data source (and its own outbox)
     * without repeating the tenant on every call. Bind once, reuse.
     *
     * <pre>{@code
     * var acme = runner.forTenant("acme");
     * acme.execute("placeOrder", principal);
     * acme.execute("ship", principal);
     * }</pre>
     *
     * @param tenant the tenant to bind; never {@code null}
     * @return a tenant-bound runner view
     */
    public TenantRunner forTenant(String tenant) {
        return new TenantRunner(this, Objects.requireNonNull(tenant, "tenant"));
    }

    /**
     * Runs {@code action} with {@code tenant} established as the ambient tenant,
     * so any {@link #execute(String, Principal)} / {@link #run(AppService, Principal)}
     * call made within (across however many layers) routes to that tenant without
     * passing it explicitly. Intended for a request boundary (e.g. a web filter).
     *
     * <p>The binding is restored on exit, even on exception. Nested
     * {@code withTenant} calls override for their own scope.
     *
     * @param tenant the tenant for the scope; never {@code null}
     * @param action the work to run; never {@code null}
     * @param <R>    the result type
     * @return the action's result
     * @throws AppServiceException if the action throws it
     */
    public <R> R withTenant(String tenant, TenantScope<R> action) throws AppServiceException {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(action, "action");
        String previous = ambientTenant.get();
        ambientTenant.set(tenant);
        try {
            return action.run();
        } finally {
            if (previous == null) ambientTenant.remove();
            else ambientTenant.set(previous);
        }
    }

    /** Work run within a {@link #withTenant} scope. */
    @FunctionalInterface
    public interface TenantScope<R> {
        /**
         * Runs the scoped work.
         *
         * @return the result
         * @throws AppServiceException if the work fails
         */
        R run() throws AppServiceException;
    }

    /**
     * A view of a {@link ServiceRunner} bound to one tenant. Obtained from
     * {@link ServiceRunner#forTenant(String)}; its {@code execute} / {@code run}
     * route to the bound tenant.
     */
    public static final class TenantRunner {

        private final ServiceRunner runner;
        private final String tenant;

        private TenantRunner(ServiceRunner runner, String tenant) {
            this.runner = runner;
            this.tenant = tenant;
        }

        /** The tenant this view is bound to. @return the tenant; never {@code null} */
        public String tenant() {
            return tenant;
        }

        /**
         * Executes a registered service for this tenant using the default locale.
         *
         * @param name      the service name; never {@code null}
         * @param principal the authenticated caller; never {@code null}
         * @param <R>       the service return type
         * @return the service result
         * @throws AppServiceException if the service or a post-commit action fails
         */
        public <R> R execute(String name, Principal principal) throws AppServiceException {
            return runner.execute(name, principal, tenant, runner.defaultLocale);
        }

        /**
         * Executes a registered service for this tenant with an explicit locale.
         *
         * @param name      the service name; never {@code null}
         * @param principal the authenticated caller; never {@code null}
         * @param locale    the request locale; never {@code null}
         * @param <R>       the service return type
         * @return the service result
         * @throws AppServiceException if the service or a post-commit action fails
         */
        public <R> R execute(String name, Principal principal, Locale locale)
                throws AppServiceException {
            return runner.execute(name, principal, tenant, locale);
        }

        /**
         * Runs an ad-hoc service for this tenant using the default locale.
         *
         * @param service   the service; never {@code null}
         * @param principal the authenticated caller; never {@code null}
         * @param <R>       the service return type
         * @return the service result
         * @throws AppServiceException if the service or a post-commit action fails
         */
        public <R> R run(AppService<R> service, Principal principal) throws AppServiceException {
            return runner.run(service, principal, tenant, runner.defaultLocale);
        }

        /**
         * Returns the number of durable events awaiting delivery in this tenant's
         * outbox, or empty if durable events are not enabled.
         *
         * @return the pending count, or empty
         */
        public java.util.OptionalLong pendingEventCount() {
            var ob = runner.outboxFor(tenant);
            return ob == null ? java.util.OptionalLong.empty()
                : java.util.OptionalLong.of(ob.pendingCount());
        }

        /**
         * Returns the number of dead-lettered durable events in this tenant's
         * outbox, or empty if durable events are not enabled.
         *
         * @return the dead-letter count, or empty
         */
        public java.util.OptionalLong deadLetterCount() {
            var ob = runner.outboxFor(tenant);
            return ob == null ? java.util.OptionalLong.empty()
                : java.util.OptionalLong.of(ob.deadLetterCount());
        }
    }

    // ── Nested (same-transaction) invocation — used by AppContext.call ─────────

    <R> R callNested(AppContext ctx, AppService<R> service) throws AppServiceException {
        try {
            return service.execute(ctx);
        } catch (AppServiceException e) {
            throw e;
        } catch (Exception e) {
            throw new AppServiceException("Nested service execution failed", e);
        }
    }

    @Override
    public void close() {
        outboxes.values().forEach(Outbox::close);
    }

    // ── Introspection (management surface) ─────────────────────────────────────

    /**
     * Returns the names of the registered services.
     *
     * @return an immutable set of service names
     */
    public java.util.Set<String> serviceNames() {
        return services.keySet();
    }

    /**
     * Returns the number of durable events awaiting delivery, when durable
     * events are enabled.
     *
     * @return the pending event count, or empty if events are delivered in-process
     */
    public java.util.OptionalLong pendingEventCount() {
        return !durableEnabled
            ? java.util.OptionalLong.empty()
            : java.util.OptionalLong.of(defaultOutbox().pendingCount());
    }

    /**
     * Returns the number of dead-lettered durable events — those that exhausted
     * their delivery attempts or could not be decoded — when durable events are
     * enabled.
     *
     * @return the dead-letter count, or empty if events are delivered in-process
     */
    public java.util.OptionalLong deadLetterCount() {
        return !durableEnabled
            ? java.util.OptionalLong.empty()
            : java.util.OptionalLong.of(defaultOutbox().deadLetterCount());
    }

    /**
     * Returns up to {@code limit} durable events still awaiting delivery, oldest
     * first, for inspection. Empty when durable events are not enabled.
     *
     * @param limit the maximum number of entries; must be &ge; 0
     * @return the pending entries (metadata only)
     */
    public List<OutboxEntry> pendingEvents(int limit) {
        return !durableEnabled ? List.of() : defaultOutbox().peekPending(limit);
    }

    /**
     * Returns up to {@code limit} dead-lettered durable events, oldest first, for
     * triage. Empty when durable events are not enabled.
     *
     * @param limit the maximum number of entries; must be &ge; 0
     * @return the dead-lettered entries (metadata only)
     */
    public List<OutboxEntry> deadLetterEvents(int limit) {
        return !durableEnabled ? List.of() : defaultOutbox().peekDeadLetters(limit);
    }

    /**
     * Requeues a dead-lettered durable event for delivery, resetting its attempt
     * count. Use after fixing whatever caused delivery to fail.
     *
     * @param id the outbox row id (from {@link #deadLetterEvents(int)})
     * @return {@code true} if a dead-lettered event was requeued; {@code false}
     *         if none matched or durable events are not enabled
     */
    public boolean retryEvent(long id) {
        return durableEnabled && defaultOutbox().retry(id);
    }

    /**
     * Permanently discards a durable event, whatever its status. Intended for
     * dead-lettered events that should not be delivered.
     *
     * @param id the outbox row id (from {@link #deadLetterEvents(int)})
     * @return {@code true} if an event was discarded; {@code false} if none
     *         matched or durable events are not enabled
     */
    public boolean discardEvent(long id) {
        return durableEnabled && defaultOutbox().discard(id);
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    /** The describer registry for {@link AppContext#store}, or {@code null} if none. */
    Repositories describers() {
        return describers;
    }

    private static String tenantKey(String tenant) {
        return tenant == null ? "" : tenant;
    }

    private DataSource dataSourceFor(String tenant) {
        return dataSources.computeIfAbsent(tenantKey(tenant), k -> {
            DataSource ds = router.apply(tenant);
            if (ds == null) {
                throw new IllegalStateException("No data source for tenant: " + tenant);
            }
            return ds;
        });
    }

    private Transactor transactorFor(String tenant) {
        return transactors.computeIfAbsent(tenantKey(tenant),
            k -> new Transactor(dataSourceFor(tenant)));
    }

    /**
     * The Outbox for {@code tenant} — one per tenant, each on that tenant's own
     * DataSource (its own {@code backbone_outbox} table and poller), created on
     * first use. {@code null} when durable events are not enabled.
     */
    private Outbox outboxFor(String tenant) {
        if (!durableEnabled) return null;
        return outboxes.computeIfAbsent(tenantKey(tenant),
            k -> new Outbox(dataSourceFor(tenant), this::deliver,
                outboxTypes, outboxPollInterval, outboxRetention, outboxMaxAttempts));
    }

    /** Outbox used by the runner-level management methods (the default tenant). */
    private Outbox defaultOutbox() {
        return outboxFor(null);
    }

    private void flushPostCommit(AppContext ctx, boolean dispatchEventsInProcess)
            throws AppServiceException {
        var errors = new ArrayList<Throwable>();
        if (dispatchEventsInProcess) {
            for (Object event : ctx.pendingEvents()) {
                dispatchCollecting(event, errors);
            }
        }
        for (Runnable action : ctx.afterCommitActions()) {
            try {
                action.run();
            } catch (Exception e) {
                errors.add(e);
            }
        }
        if (!errors.isEmpty()) {
            var ex = new AppServiceException("One or more post-commit actions failed");
            errors.forEach(ex::addSuppressed);
            throw ex;
        }
    }

    @SuppressWarnings("unchecked")
    private void dispatchCollecting(Object event, List<Throwable> errors) {
        for (Subscription<?> sub : subscriptions) {
            if (sub.type().isInstance(event)) {
                try {
                    ((Consumer<Object>) sub.listener()).accept(event);
                } catch (Exception e) {
                    errors.add(e);
                }
            }
        }
    }

    /** Outbox deliverer: dispatch to subscribers, logging (not aggregating) failures. */
    @SuppressWarnings("unchecked")
    private void deliver(Object event) {
        for (Subscription<?> sub : subscriptions) {
            if (sub.type().isInstance(event)) {
                ((Consumer<Object>) sub.listener()).accept(event);
            }
        }
    }

    /** Carries an {@link AppServiceException} out of the transaction lambda. */
    private static final class ServiceFailure extends RuntimeException {
        private final AppServiceException appException;

        ServiceFailure(AppServiceException appException) {
            super(appException);
            this.appException = appException;
        }
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    /**
     * Builder for {@link ServiceRunner}.
     */
    public static final class Builder {

        private Function<String, DataSource> router;
        private Locale defaultLocale = Locale.getDefault();
        private final Map<String, AppService<?>> services = new HashMap<>();
        private final List<Subscription<?>> subscriptions = new ArrayList<>();
        private final List<Class<?>> outboxTypes = new ArrayList<>();
        private Duration outboxPollInterval = Duration.ofMillis(200);
        private Duration outboxRetention = Duration.ofDays(7);
        private int outboxMaxAttempts = Outbox.DEFAULT_MAX_ATTEMPTS;
        private Repositories describers;

        private Builder() {}

        /**
         * Configures a single data source for all (single-tenant) requests.
         *
         * @param dataSource the JDBC data source; never {@code null}
         * @return this builder
         */
        public Builder dataSource(DataSource dataSource) {
            Objects.requireNonNull(dataSource, "dataSource");
            this.router = tenant -> dataSource;
            return this;
        }

        /**
         * Configures per-tenant data-source routing. The function maps a tenant
         * id (possibly {@code null}) to the data source to use.
         *
         * @param tenantRouter the routing function; never {@code null}
         * @return this builder
         */
        public Builder tenantRouter(Function<String, DataSource> tenantRouter) {
            this.router = Objects.requireNonNull(tenantRouter, "tenantRouter");
            return this;
        }

        /**
         * Sets the default locale used when none is supplied to {@code execute}/{@code run}.
         *
         * @param locale the default locale; never {@code null}
         * @return this builder
         */
        public Builder defaultLocale(Locale locale) {
            this.defaultLocale = Objects.requireNonNull(locale, "locale");
            return this;
        }

        /**
         * Registers a named service.
         *
         * @param name    the service identifier; never {@code null}
         * @param service the service implementation; never {@code null}
         * @param <R>     the service return type
         * @return this builder
         */
        public <R> Builder register(String name, AppService<R> service) {
            services.put(
                Objects.requireNonNull(name,    "name"),
                Objects.requireNonNull(service, "service"));
            return this;
        }

        /**
         * Subscribes a listener to domain events of the given type, delivered
         * after a service's transaction commits.
         *
         * @param type     the event type to receive; never {@code null}
         * @param listener the listener; never {@code null}
         * @param <E>      the event type
         * @return this builder
         */
        public <E> Builder subscribe(Class<E> type, Consumer<E> listener) {
            subscriptions.add(new Subscription<>(
                Objects.requireNonNull(type,     "type"),
                Objects.requireNonNull(listener, "listener")));
            return this;
        }

        /**
         * Enables durable, at-least-once event delivery via a transactional
         * outbox for the given (serializable) event types. Without this, events
         * are delivered in-process after commit and lost on a crash.
         *
         * @param eventTypes the serializable event classes to persist
         * @return this builder
         */
        public Builder durableEvents(Class<?>... eventTypes) {
            for (var t : eventTypes) {
                outboxTypes.add(Objects.requireNonNull(t, "eventType"));
            }
            return this;
        }

        /**
         * Sets how long the outbox poller waits when idle (default 200&nbsp;ms).
         *
         * @param interval the poll interval; never {@code null}
         * @return this builder
         */
        public Builder outboxPollInterval(Duration interval) {
            this.outboxPollInterval = Objects.requireNonNull(interval, "interval");
            return this;
        }

        /**
         * Sets how long processed outbox rows are retained before purging
         * (default 7&nbsp;days).
         *
         * @param retention the retention duration; never {@code null}
         * @return this builder
         */
        public Builder outboxRetention(Duration retention) {
            this.outboxRetention = Objects.requireNonNull(retention, "retention");
            return this;
        }

        /**
         * Sets how many times the outbox attempts to deliver an event before
         * moving it to the dead-letter state (default
         * {@value net.teppan.backbone.event.Outbox#DEFAULT_MAX_ATTEMPTS}).
         *
         * @param maxAttempts the attempt limit; must be &ge; 1
         * @return this builder
         */
        public Builder outboxMaxAttempts(int maxAttempts) {
            if (maxAttempts < 1) {
                throw new IllegalArgumentException("maxAttempts must be >= 1: " + maxAttempts);
            }
            this.outboxMaxAttempts = maxAttempts;
            return this;
        }

        /**
         * Registers a describer registry so services can store and read by type
         * via {@link AppContext#store(Object...)} and friends, instead of naming
         * a describer on every call.
         *
         * @param describers the registry; never {@code null}
         * @return this builder
         */
        public Builder describers(Repositories describers) {
            this.describers = Objects.requireNonNull(describers, "describers");
            return this;
        }

        /**
         * Builds the {@link ServiceRunner}.
         *
         * @return a new runner
         * @throws IllegalStateException if neither a data source nor a tenant
         *                               router was configured
         */
        public ServiceRunner build() {
            if (router == null) {
                throw new IllegalStateException("a dataSource or tenantRouter must be set");
            }
            return new ServiceRunner(router, defaultLocale, services, subscriptions,
                outboxTypes, outboxPollInterval, outboxRetention,
                outboxMaxAttempts, describers);
        }
    }
}
