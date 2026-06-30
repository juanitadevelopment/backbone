package net.teppan.backbone.management;

import net.teppan.backbone.ServiceRunner;
import net.teppan.backbone.event.OutboxEntry;
import net.teppan.backbone.timer.TimerScheduler;
import net.teppan.backbone.timer.TimerScheduler.JobStatus;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * A single operational surface over a running backbone.
 *
 * <p>A backbone is assembled from independent parts — a {@link ServiceRunner}
 * (services and the durable-event outbox) and, optionally, a
 * {@link TimerScheduler} (scheduled jobs). At run time an operator needs one
 * place to see their combined state and to act on it. {@code BackboneConsole}
 * binds those parts and exposes that surface:
 *
 * <ul>
 *   <li>{@link #snapshot()} — a combined view of services, outbox counts, and
 *       job statuses, for a monitoring dashboard;</li>
 *   <li>outbox triage — {@link #pendingEvents(int)}, {@link #deadLetters(int)},
 *       {@link #retry(long)}, {@link #discard(long)}, and the bulk
 *       {@link #retryAllDeadLetters()};</li>
 *   <li>job control — {@link #suspendJob(String)}, {@link #resumeJob(String)},
 *       {@link #cancelJob(String)}, and the bulk {@link #suspendAllJobs()} /
 *       {@link #resumeAllJobs()}.</li>
 * </ul>
 *
 * <p>This is a typed API, not a wire protocol: bind it to a CLI, an admin HTTP
 * endpoint, or JMX as the application sees fit. The console holds no resources
 * of its own and does not own the runner or scheduler — closing those remains
 * the caller's responsibility.
 *
 * <p>Methods that operate on jobs require a scheduler to have been attached;
 * they throw {@link IllegalStateException} otherwise. The bulk job operations
 * and {@link #jobStatuses()} instead treat an absent scheduler as "no jobs".
 */
public final class BackboneConsole {

    private static final int DEAD_LETTER_BATCH = 100;

    private final ServiceRunner runner;
    private final TimerScheduler scheduler;  // nullable

    private BackboneConsole(ServiceRunner runner, TimerScheduler scheduler) {
        this.runner = Objects.requireNonNull(runner, "runner");
        this.scheduler = scheduler;
    }

    /**
     * Returns a new builder.
     *
     * @return a builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    // ── Combined view ──────────────────────────────────────────────────────────

    /**
     * Captures the current state of the backbone: registered services, durable
     * event counts, and job statuses.
     *
     * @return a snapshot for monitoring
     */
    public ConsoleSnapshot snapshot() {
        return new ConsoleSnapshot(
            runner.serviceNames(),
            runner.pendingEventCount(),
            runner.deadLetterCount(),
            jobStatuses());
    }

    // ── Outbox triage ──────────────────────────────────────────────────────────

    /**
     * Returns up to {@code limit} durable events awaiting delivery, oldest first.
     *
     * @param limit the maximum number of entries; must be &ge; 0
     * @return pending entries (metadata only); empty without durable events
     */
    public List<OutboxEntry> pendingEvents(int limit) {
        return runner.pendingEvents(limit);
    }

    /**
     * Returns up to {@code limit} dead-lettered durable events, oldest first.
     *
     * @param limit the maximum number of entries; must be &ge; 0
     * @return dead-lettered entries (metadata only); empty without durable events
     */
    public List<OutboxEntry> deadLetters(int limit) {
        return runner.deadLetterEvents(limit);
    }

    /**
     * Requeues one dead-lettered event for delivery.
     *
     * @param id the outbox row id
     * @return {@code true} if a dead-lettered event was requeued
     */
    public boolean retry(long id) {
        return runner.retryEvent(id);
    }

    /**
     * Permanently discards one durable event.
     *
     * @param id the outbox row id
     * @return {@code true} if an event was discarded
     */
    public boolean discard(long id) {
        return runner.discardEvent(id);
    }

    /**
     * Requeues every dead-lettered event for delivery. Useful after fixing a
     * downstream that caused a batch of failures.
     *
     * @return the number of events requeued
     */
    public int retryAllDeadLetters() {
        int total = 0;
        while (true) {
            var batch = runner.deadLetterEvents(DEAD_LETTER_BATCH);
            if (batch.isEmpty()) break;
            int before = total;
            for (var e : batch) {
                if (runner.retryEvent(e.id())) total++;
            }
            if (total == before) break;  // no progress; avoid spinning
        }
        return total;
    }

    // ── Job control ────────────────────────────────────────────────────────────

    /**
     * Returns each scheduled job's name and current status, or an empty map when
     * no scheduler is attached.
     *
     * @return an immutable map of job name to status
     */
    public Map<String, JobStatus> jobStatuses() {
        return scheduler == null ? Map.of() : scheduler.jobStatuses();
    }

    /**
     * Suspends one running job.
     *
     * @param name the job name; never {@code null}
     * @throws IllegalStateException    if no scheduler is attached
     * @throws IllegalArgumentException if the job is not registered
     */
    public void suspendJob(String name) {
        requireScheduler().suspend(name);
    }

    /**
     * Resumes one suspended job.
     *
     * @param name the job name; never {@code null}
     * @throws IllegalStateException    if no scheduler is attached, or the job is
     *                                  not suspended
     * @throws IllegalArgumentException if the job is not registered
     */
    public void resumeJob(String name) {
        requireScheduler().resume(name);
    }

    /**
     * Permanently cancels one job.
     *
     * @param name the job name; never {@code null}
     * @throws IllegalStateException    if no scheduler is attached
     * @throws IllegalArgumentException if the job is not registered
     */
    public void cancelJob(String name) {
        requireScheduler().cancel(name);
    }

    /**
     * Suspends every currently running job.
     *
     * @return the number of jobs suspended (0 when no scheduler is attached)
     */
    public int suspendAllJobs() {
        if (scheduler == null) return 0;
        int count = 0;
        for (var e : scheduler.jobStatuses().entrySet()) {
            if (e.getValue() == JobStatus.RUNNING) {
                scheduler.suspend(e.getKey());
                count++;
            }
        }
        return count;
    }

    /**
     * Resumes every currently suspended job.
     *
     * @return the number of jobs resumed (0 when no scheduler is attached)
     */
    public int resumeAllJobs() {
        if (scheduler == null) return 0;
        int count = 0;
        for (var e : scheduler.jobStatuses().entrySet()) {
            if (e.getValue() == JobStatus.SUSPENDED) {
                scheduler.resume(e.getKey());
                count++;
            }
        }
        return count;
    }

    private TimerScheduler requireScheduler() {
        if (scheduler == null) {
            throw new IllegalStateException("No scheduler is attached to this console");
        }
        return scheduler;
    }

    // ── Builder ────────────────────────────────────────────────────────────────

    /**
     * Builder for {@link BackboneConsole}.
     */
    public static final class Builder {

        private ServiceRunner runner;
        private TimerScheduler scheduler;

        private Builder() {}

        /**
         * Binds the service runner to manage (required).
         *
         * @param runner the running service runner; never {@code null}
         * @return this builder
         */
        public Builder serviceRunner(ServiceRunner runner) {
            this.runner = Objects.requireNonNull(runner, "runner");
            return this;
        }

        /**
         * Binds a scheduler whose jobs this console can inspect and control
         * (optional).
         *
         * @param scheduler the running scheduler; never {@code null}
         * @return this builder
         */
        public Builder scheduler(TimerScheduler scheduler) {
            this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
            return this;
        }

        /**
         * Builds the console.
         *
         * @return a new console
         * @throws IllegalStateException if no service runner was set
         */
        public BackboneConsole build() {
            if (runner == null) {
                throw new IllegalStateException("a serviceRunner must be set");
            }
            return new BackboneConsole(runner, scheduler);
        }
    }
}
