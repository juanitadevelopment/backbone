package net.teppan.backbone;

import net.teppan.backbone.event.Outbox;
import net.teppan.shazo.ShazoException;
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
    private final Outbox outbox;  // null when events are delivered in-process

    private record Subscription<E>(Class<E> type, Consumer<E> listener) {}

    private ServiceRunner(Function<String, DataSource> router, Locale defaultLocale,
                          Map<String, AppService<?>> services,
                          List<Subscription<?>> subscriptions,
                          DataSource outboxDataSource, List<Class<?>> outboxTypes,
                          Duration pollInterval, Duration retention) {
        this.router        = router;
        this.defaultLocale = defaultLocale;
        this.services      = Map.copyOf(services);
        this.subscriptions = List.copyOf(subscriptions);
        if (outboxTypes.isEmpty()) {
            this.outbox = null;
        } else {
            if (outboxDataSource == null) {
                throw new IllegalStateException("durableEvents requires a dataSource(...)");
            }
            this.outbox = new Outbox(outboxDataSource, this::deliver,
                outboxTypes, pollInterval, retention);
        }
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
        return execute(name, principal, null, defaultLocale);
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
        return run(service, principal, null, defaultLocale);
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

        var ctxRef = new AtomicReference<AppContext>();
        R result;
        try {
            result = transactorFor(tenant).execute(uow -> {
                var ctx = new AppContext(uow, principal, tenant, locale, this);
                ctxRef.set(ctx);
                try {
                    R r = service.execute(ctx);
                    if (outbox != null) {
                        // Persist events in the same transaction as the change.
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
        if (outbox != null) {
            outbox.close();
        }
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
        return outbox == null
            ? java.util.OptionalLong.empty()
            : java.util.OptionalLong.of(outbox.pendingCount());
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    private Transactor transactorFor(String tenant) {
        String key = (tenant == null) ? "" : tenant;
        return transactors.computeIfAbsent(key, k -> {
            DataSource ds = router.apply(tenant);
            if (ds == null) {
                throw new IllegalStateException("No data source for tenant: " + tenant);
            }
            return new Transactor(ds);
        });
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
        private DataSource singleDataSource;   // backs the outbox, if enabled
        private Locale defaultLocale = Locale.getDefault();
        private final Map<String, AppService<?>> services = new HashMap<>();
        private final List<Subscription<?>> subscriptions = new ArrayList<>();
        private final List<Class<?>> outboxTypes = new ArrayList<>();
        private Duration outboxPollInterval = Duration.ofMillis(200);
        private Duration outboxRetention = Duration.ofDays(7);

        private Builder() {}

        /**
         * Configures a single data source for all (single-tenant) requests; also
         * used to back the outbox when durable events are enabled.
         *
         * @param dataSource the JDBC data source; never {@code null}
         * @return this builder
         */
        public Builder dataSource(DataSource dataSource) {
            Objects.requireNonNull(dataSource, "dataSource");
            this.singleDataSource = dataSource;
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
                singleDataSource, outboxTypes, outboxPollInterval, outboxRetention);
        }
    }
}
