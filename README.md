# Backbone

A minimal **transactional service and domain-event runtime** for Java, built on
the [shazo](https://github.com/juanitadevelopment/shazo) persistence abstraction.

Backbone gives a plain Java application the small set of "application server"
services that a data-driven system keeps re-inventing — a transactional unit of
work per request, composable services, domain events delivered *after* commit,
scheduled jobs, and basic runtime introspection — without a heavyweight
container, dynamic proxies, or XML.

> **Status:** early release (`0.1.0`). API may still change before `1.0.0`.

## Requirements

- **Java 25+**
- **shazo `0.1.1`** — resolved automatically from JitPack (see [Getting shazo](#getting-shazo))

## What it gives you

| Concern | Type | Summary |
|---|---|---|
| Transactional service runner | `ServiceRunner` | Runs each service as one transaction; commits, then delivers events |
| Request context / unit of work | `AppContext` | Transaction-scoped repositories, principal, tenant, locale |
| Service unit | `AppService<R>` | A lambda `AppContext -> R` |
| Identity | `Principal` | Immutable id + roles; `anonymous()` / `system()` |
| Domain events | `ServiceRunner.subscribe` + `Outbox` | In-process or durable (transactional outbox), delivered after commit |
| Scheduling | `TimerScheduler`, `CronExpression` | Interval and 6-field cron jobs as system units of work |

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

## Scheduling

```java
try (var scheduler = TimerScheduler.builder().dataSource(dataSource).build()) {
    scheduler.schedule("nightly", "0 0 2 * * *", ctx -> cleanup(ctx)); // 6-field cron
    scheduler.schedule("heartbeat", Duration.ofSeconds(30), ctx -> ping(ctx));
}
```

Each job runs as a `Principal.system()` unit of work; jobs can be
`suspend`/`resume`/`cancel`led and inspected via `jobStatuses()`.

## Getting shazo

Backbone depends on shazo, resolved from [JitPack](https://jitpack.io) — no
manual install needed. The build already declares:

```kotlin
repositories { maven { url = uri("https://jitpack.io") } }
dependencies { api("com.github.juanitadevelopment:shazo:v0.1.1") }
```

For offline development you can instead build shazo locally
(`./gradlew publishToMavenLocal` in the shazo project) and add `mavenLocal()`
with the `net.teppan:shazo:0.1.1` coordinate.

## Using backbone as a dependency

Backbone is itself published via JitPack:

```kotlin
repositories { maven { url = uri("https://jitpack.io") } }
dependencies { implementation("com.github.juanitadevelopment:backbone:v0.1.0") }
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
