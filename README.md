# Backbone

A minimal **transactional service and domain-event runtime** for Java, built on
the [shazo](https://github.com/juanitadevelopment/shazo) persistence abstraction.

Backbone gives a plain Java application the small set of "application server"
services that a data-driven system keeps re-inventing — a transactional unit of
work per request, composable services, domain events delivered *after* commit,
scheduled jobs, and basic runtime introspection — without a heavyweight
container, dynamic proxies, or XML.

> **Status:** early release (`0.1.4`). API may still change before `1.0.0`.

## Requirements

- **Java 25+**
- **shazo `0.1.2`** — resolved automatically from JitPack (see [Getting shazo](#getting-shazo))

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
        .durableEvents(OrderPlaced.class)                 // at-least-once, survives restart
        .subscribe(OrderPlaced.class, e -> mailer.confirm(e.orderId()))
        .register("placeOrder", ctx -> {
            ctx.repository(orderDescriber).store(order);   // transaction-scoped
            ctx.repository(auditDescriber).store(audit);   // commits atomically with the order
            ctx.publish(new OrderPlaced(order.id()));      // delivered only after commit
            return order.id();
        })
        .build()) {

    String id = runner.execute("placeOrder", principal);
}
```

- **Atomic across repositories** — every `ctx.repository(...)` shares the transaction.
- **Events after commit** — published events reach subscribers only once the
  transaction commits; a failure rolls everything back and discards them.
- **Nested composition** — `ctx.call(otherService)` joins the same transaction.
- **Multitenancy** — configure `tenantRouter(tenant -> dataSource)` and pass a
  tenant to `execute` / `run`.

## Durable events (transactional outbox)

With `durableEvents(...)`, published events are written to a `backbone_outbox`
table **in the same transaction** as the business change, then delivered
asynchronously by a poller — at-least-once, surviving restarts. There is never a
committed change without its event, nor an event without its change. Subscribers
must be idempotent. (Without `durableEvents`, events are delivered in-process,
synchronously, right after commit.)

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
dependencies { api("com.github.juanitadevelopment:shazo:v0.1.2") }
```

For offline development you can instead build shazo locally
(`./gradlew publishToMavenLocal` in the shazo project) and add `mavenLocal()`
with the `net.teppan:shazo:0.1.2` coordinate.

## Using backbone as a dependency

Backbone is itself published via JitPack:

```kotlin
repositories { maven { url = uri("https://jitpack.io") } }
dependencies { implementation("com.github.juanitadevelopment:backbone:v0.1.4") }
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
