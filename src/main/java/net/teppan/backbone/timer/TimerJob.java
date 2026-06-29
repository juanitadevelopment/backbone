package net.teppan.backbone.timer;

import net.teppan.backbone.AppContext;

/**
 * A scheduled background task executed by {@link TimerScheduler}.
 *
 * <p>Implement as a lambda; the job runs as a {@link net.teppan.backbone.Principal#system()}
 * unit of work, receiving a transactional {@link AppContext}.
 *
 * <pre>{@code
 * TimerJob cleanup = ctx -> {
 *     ctx.repository(expiredSessionDescriber).delete(new ExpiredSession());
 * };
 * scheduler.schedule("session-cleanup", Duration.ofHours(1), cleanup);
 * }</pre>
 *
 * @see TimerScheduler
 */
@FunctionalInterface
public interface TimerJob {

    /**
     * Executes this job.
     *
     * <p>Exceptions are caught and logged by {@link TimerScheduler}; they do
     * not affect subsequent executions.
     *
     * @param ctx the system context for this execution; never {@code null}
     * @throws Exception if the job fails
     */
    void run(AppContext ctx) throws Exception;
}
