package net.teppan.backbone;

/**
 * A unit of application logic that runs within a transactional service context.
 *
 * <p>Implement this interface (usually as a lambda) and run it through a
 * {@link ServiceRunner}. The runner opens a {@linkplain AppContext unit of
 * work}, invokes the service, commits the transaction, and only then delivers
 * any events the service {@linkplain AppContext#publish published} and runs its
 * {@linkplain AppContext#afterCommit after-commit} actions. If the service
 * throws, the transaction is rolled back and those deferred actions are
 * discarded.
 *
 * <p>Within a service, obtain repositories from the context — they share the
 * transaction, so writes across several domain types commit atomically:
 *
 * <pre>{@code
 * AppService<String> createOrder = ctx -> {
 *     var orders = ctx.repository(orderDescriber);
 *     var audit  = ctx.repository(auditDescriber);
 *     orders.store(order);                       // same transaction
 *     audit.store(new AuditLog(order.id(), "created"));
 *     ctx.publish(new OrderCreated(order.id())); // delivered after commit
 *     return order.id();
 * };
 * }</pre>
 *
 * <p>A service may throw any exception; the {@link ServiceRunner} rolls the
 * transaction back and reports it as an {@link AppServiceException}. Throw
 * {@link AppServiceException} directly for expected business failures.
 *
 * @param <R> the return type of this service; use {@link Void} for void services
 * @see ServiceRunner
 * @see AppContext
 */
@FunctionalInterface
public interface AppService<R> {

    /**
     * Executes this service within the given context.
     *
     * @param ctx the transactional request context; never {@code null}
     * @return the service result; may be {@code null} for {@link Void} services
     * @throws Exception if the service fails; triggers a transaction rollback
     */
    R execute(AppContext ctx) throws Exception;
}
