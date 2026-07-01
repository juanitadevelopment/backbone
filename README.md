# Backbone

> [!IMPORTANT]
> **Moved.** backbone now lives in the **[`sharaku`](https://github.com/juanitadevelopment/sharaku)**
> monorepo (alongside `shazo`) and is released from there. This repository is
> archived. Update your dependency coordinate to:
> `com.github.juanitadevelopment.sharaku:backbone:0.2.0`

A minimal **transactional service and domain-event runtime** for Java, built on
the [shazo](https://github.com/juanitadevelopment/shazo) persistence abstraction.

Backbone gives a plain Java application the small set of "application server"
services that a data-driven system keeps re-inventing — a transactional unit of
work per request, composable services, domain events delivered *after* commit,
scheduled jobs, and basic runtime introspection — without a heavyweight
container, dynamic proxies, or XML.

> **Status:** early release (`0.1.10`). API may still change before `1.0.0`.

## Requirements

- **Java 21+**
- **shazo `0.1.6`** — resolved automatically from JitPack (see [Getting shazo](#getting-shazo))

## What it gives you

| Concern | Type | Summary |
|---|---|---|
| Transactional service runner | `ServiceRunner` | Runs each service as one transaction; commits, then delivers events |
| Request context / unit of work | `AppContext` | Transaction-scoped repositories, principal, tenant, locale |
| Service unit | `AppService<R>` | A lambda `AppContext -> R` |
| Identity | `Principal` | Immutable id + roles; `anonymous()` / `system()` |
| Domain events | `ServiceRunner.subscribe` + `Outbox` | In-process or durable (transactional outbox), delivered after commit |
| Scheduling | `TimerScheduler`, `CronExpression` | Interval, 6-field cron, and one-shot (deadline) jobs as system units of work |
| Operations | `BackboneConsole` | One surface to view and control the runner and scheduler at run time |

## Core idea: a service is a transaction

```java
record Order(String id, String customer, String status) {}
record OrderPlaced(String orderId) implements java.io.Serializable {}

try (var runner = ServiceRunner.builder()
        .dataSource(dataSource)
        .describers(Repositories.builder()                 // storage binding lives in the wiring
            .register(Order.class, orderDescriber)
            .register(Audit.class, auditDescriber)
            .build())
        .durableEvents(OrderPlaced.class)                 // at-least-once, survives restart
        .subscribe(OrderPlaced.class, e -> mailer.confirm(e.orderId()))
        .register("placeOrder", ctx -> {
            ctx.store(order, audit);                       // both types, one transaction
            ctx.publish(new OrderPlaced(order.id()));      // delivered only after commit
            return order.id();
        })
        .build()) {

    String id = runner.execute("placeOrder", principal);
}
```

Services name only domain types (`ctx.store(order)`, `ctx.repository(Order.class)`,
`ctx.retrieve(Order.class, key)`); which describer — and therefore which storage —
backs each type is configured once, in the wiring. No JDBC types leak into service code.

- **Atomic across types** — everything a service stores shares the transaction.
- **Events after commit** — published events reach subscribers only once the
  transaction commits; a failure rolls everything back and discards them.
- **Nested composition** — `ctx.call(otherService)` joins the same transaction.
- **Multitenancy** — `tenantRouter(tenant -> dataSource)` plus `forTenant(...)` /
  `withTenant(...)` (see [Multi-tenancy](#multi-tenancy)).

### The describer registry (storage in the wiring, not in services)

A service should not name the storage. With a
[`Repositories`](https://github.com/juanitadevelopment/shazo) registry, the
describers (which carry the JDBC command type) are declared once at wiring time,
and services address persistence purely by domain type — `ctx.store(...)`
(varargs), `ctx.delete(...)`, `ctx.repository(Order.class)`, and
`ctx.retrieve(Order.class, key)`:

```java
var describers = Repositories.builder()
    .register(Order.class,   orderDescriber)
    .register(Booking.class, bookingDescriber)
    .build();

var runner = ServiceRunner.builder().dataSource(ds).describers(describers) /* ... */ .build();

// inside a service:
ctx.store(order, booking);                       // both types, one transaction
Repository<Order> orders = ctx.repository(Order.class);
Optional<Order> o = ctx.retrieve(Order.class, new Order(id, null));
```

## Durable events (transactional outbox)

With `durableEvents(...)`, published events are written to a `backbone_outbox`
table **in the same transaction** as the business change, then delivered
asynchronously by a poller — at-least-once, surviving restarts. There is never a
committed change without its event, nor an event without its change. Subscribers
must be idempotent. (Without `durableEvents`, events are delivered in-process,
synchronously, right after commit.) `ctx.publish(...)` is varargs, so several
events can be raised in one call: `ctx.publish(orderPlaced, bookingConfirmed)`.

### Dead-letters and triage

An event that keeps failing is not retried forever. After
`outboxMaxAttempts(...)` failed deliveries (default `10`) it moves to a terminal
**dead-letter** state and the poller skips it; an event whose payload cannot be
decoded is dead-lettered immediately. Dead-lettered events can be inspected and,
once the cause is fixed, requeued or discarded:

```java
runner.deadLetterCount();              // OptionalLong
for (OutboxEntry e : runner.deadLetterEvents(50)) {
    log.warn("stuck event {} ({}) after {} attempts: {}",
        e.id(), e.type(), e.attempts(), e.lastError().orElse(""));
}
runner.retryEvent(id);                 // requeue (resets the attempt count)
runner.discardEvent(id);              // drop permanently
```

`pendingEvents(int)` lists events still awaiting delivery the same way.

## Multi-tenancy

Configure how a tenant maps to a data source, then bind the tenant **once** —
never on every call:

```java
var runner = ServiceRunner.builder()
        .tenantRouter(tenant -> dataSourceFor(tenant))   // tenant -> DataSource
        .describers(describers)
        .durableEvents(OrderPlaced.class)
        .register("placeOrder", ctx -> { ctx.store(order); return order.id(); })
        .build();

// bind once, reuse — routes to acme's data source (and its own outbox)
var acme = runner.forTenant("acme");
acme.execute("placeOrder", principal);
acme.execute("ship", principal);

// or establish an ambient tenant for a request boundary (e.g. a web filter):
runner.withTenant("acme", () -> {
    runner.execute("placeOrder", principal);   // picks up "acme" implicitly
    return null;
});
```

Inside a service, `ctx.tenant()` reports the current tenant. Durable events are
**per tenant**: each tenant's events are written to and polled from that tenant's
own data source (its own `backbone_outbox`).

The mapping itself is your choice, all behind the same `tenant -> DataSource` seam:

| Strategy | How `tenantRouter` returns the data source |
|---|---|
| **database-per-tenant** | a distinct `DataSource` per tenant (no wrapper) |
| **schema-per-tenant** | `new SessionInitDataSource(shared, "SET SCHEMA " + tenant)` |
| **row-level security** | `new SessionInitDataSource(shared, "SET app.current_tenant = '" + tenant + "'")` — the database's RLS policies enforce isolation |

[`SessionInitDataSource`](https://github.com/juanitadevelopment/shazo) is from
shazo. The same application code runs unchanged across strategies — e.g. schema
isolation on H2 in tests, PostgreSQL RLS in production.

## Scheduling

```java
try (var scheduler = TimerScheduler.builder().dataSource(dataSource).build()) {
    scheduler.schedule("nightly", "0 0 2 * * *", ctx -> cleanup(ctx)); // 6-field cron
    scheduler.schedule("heartbeat", Duration.ofSeconds(30), ctx -> ping(ctx));

    // one-shot deadline: expire this request in 48 hours (fires once, then COMPLETED)
    scheduler.schedule("expire-" + id, Instant.now().plus(Duration.ofHours(48)),
        ctx -> approvals.expire(ctx, id));
}
```

Each job runs as a `Principal.system()` unit of work; jobs can be
`suspend`/`resume`/`cancel`led and inspected via `jobStatuses()`. One-shot jobs
live in memory only — for deadlines that must survive a restart, persist them and
rearm on startup.

### Tenant-scoped and fan-out jobs

A scheduler built with `tenantRouter(...)` can bind a job to one tenant, or fan a
single schedule out across many — each firing runs as its own tenant-scoped unit
of work (its `ctx` routes to that tenant's data source):

```java
try (var scheduler = TimerScheduler.builder().tenantRouter(router).build()) {
    // one tenant
    scheduler.schedule("acme-nightly", "0 0 2 * * *", "acme", ctx -> cleanup(ctx));

    // every tenant, re-resolved at each firing (add a tenant, it's picked up)
    scheduler.scheduleForEachTenant("nightly", Duration.ofHours(24),
        () -> tenantRegistry.activeTenants(),
        ctx -> cleanup(ctx));   // ctx.tenant() reports which tenant this run is for
}
```

## Operations console

A backbone is assembled from independent parts; `BackboneConsole` binds them into
one operational surface, so a CLI, an admin HTTP endpoint, or JMX can see and
control the live system from a single object.

```java
var console = BackboneConsole.builder()
        .serviceRunner(runner)
        .scheduler(scheduler)        // optional
        .build();

ConsoleSnapshot s = console.snapshot();   // services, outbox counts, job statuses

// Outbox triage
console.deadLetters(50);                   // inspect stuck events
console.retryAllDeadLetters();             // requeue them after a fix

// Job control
console.suspendAllJobs();                  // e.g. before maintenance
console.resumeAllJobs();
console.suspendJob("nightly");
```

The console is a typed API and holds no resources of its own; it does not own
the runner or scheduler, so closing those remains the caller's responsibility.

## Getting shazo

Backbone depends on shazo, resolved from [JitPack](https://jitpack.io) — no
manual install needed. The build already declares:

```kotlin
repositories { maven { url = uri("https://jitpack.io") } }
dependencies { api("com.github.juanitadevelopment:shazo:v0.1.6") }
```

For offline development you can instead build shazo locally
(`./gradlew publishToMavenLocal` in the shazo project) and add `mavenLocal()`
with the `net.teppan:shazo:0.1.6` coordinate.

## Using backbone as a dependency

Backbone is itself published via JitPack:

```kotlin
repositories { maven { url = uri("https://jitpack.io") } }
dependencies { implementation("com.github.juanitadevelopment:backbone:v0.1.10") }
```

JitPack builds shazo transitively, so a single dependency is enough.

## Build

```sh
./gradlew test      # run the test suite
./gradlew jar       # build the library jar
./gradlew javadoc   # generate API docs
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE)
for attribution.
