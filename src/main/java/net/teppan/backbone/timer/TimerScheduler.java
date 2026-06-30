package net.teppan.backbone.timer;

import net.teppan.backbone.AppContext;
import net.teppan.backbone.AppServiceException;
import net.teppan.backbone.Principal;
import net.teppan.backbone.ServiceRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Interval, cron, and one-shot scheduler for {@link TimerJob} instances.
 *
 * <p>Each job runs on a virtual thread provided by a
 * {@link ScheduledExecutorService}. Jobs receive a system {@link AppContext}
 * built from the scheduler's configured {@link DataSource}.
 *
 * <p>Jobs can be suspended (paused) and resumed without losing their
 * schedule configuration.
 *
 * <pre>{@code
 * var scheduler = TimerScheduler.builder()
 *     .dataSource(dataSource)
 *     .build();
 *
 * // interval-based
 * scheduler.schedule("heartbeat", Duration.ofSeconds(30),
 *     ctx -> log.info("alive"));
 *
 * // cron-based (every day at 02:00)
 * scheduler.schedule("nightly-cleanup", "0 0 2 * * *",
 *     ctx -> cleanupService.run(ctx));
 *
 * // one-shot (a deadline: expire this request in 48 hours)
 * scheduler.schedule("expire-" + id, Instant.now().plus(Duration.ofHours(48)),
 *     ctx -> approvals.expire(ctx, id));
 *
 * scheduler.suspend("heartbeat");
 * scheduler.resume("heartbeat");
 * scheduler.cancel("heartbeat");
 * }</pre>
 */
public final class TimerScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TimerScheduler.class);

    /** Visible job states returned by {@link #status(String)}. */
    public enum JobStatus {
        /** The job is active and will fire on its next scheduled time. */
        RUNNING,
        /** The job has been paused; no executions occur until {@link TimerScheduler#resume} is called. */
        SUSPENDED,
        /** The job has been permanently stopped and cannot be resumed. */
        CANCELLED,
        /** A one-shot job that has already fired. */
        COMPLETED
    }

    /** Default number of scheduler threads (jobs run concurrently up to this). */
    public static final int DEFAULT_POOL_SIZE = 4;

    private final ServiceRunner runner;
    private final Locale locale;
    private final ScheduledExecutorService executor;
    private final Map<String, JobEntry> jobs = new ConcurrentHashMap<>();

    private TimerScheduler(DataSource dataSource, Locale locale, int poolSize) {
        this.runner   = ServiceRunner.builder().dataSource(dataSource).defaultLocale(locale).build();
        this.locale   = locale;
        // A pool larger than one so a long-running job does not starve the others.
        this.executor = Executors.newScheduledThreadPool(
            poolSize, Thread.ofVirtual().name("timer-job-", 0).factory());
    }

    /**
     * Returns a new builder for {@code TimerScheduler}.
     *
     * @return a builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    // ── Scheduling ────────────────────────────────────────────────────────────

    /**
     * Schedules a job to run repeatedly at the given fixed interval.
     *
     * <p>The first execution starts after one interval has elapsed.
     *
     * @param name     a unique job name; never {@code null}
     * @param interval the period between executions; never {@code null}
     * @param job      the task to run; never {@code null}
     * @throws IllegalArgumentException if {@code name} is already registered
     */
    public void schedule(String name, Duration interval, TimerJob job) {
        Objects.requireNonNull(name,     "name");
        Objects.requireNonNull(interval, "interval");
        Objects.requireNonNull(job,      "job");
        checkNotRegistered(name);

        long millis = interval.toMillis();
        Future<?> future = executor.scheduleAtFixedRate(
            () -> runJob(name, job), millis, millis, TimeUnit.MILLISECONDS);
        jobs.put(name, new JobEntry(job, null, interval, null, future, JobStatus.RUNNING));
    }

    /**
     * Schedules a job to run according to a 6-field cron expression.
     *
     * @param name           a unique job name; never {@code null}
     * @param cronExpression the cron expression (second minute hour dom month dow);
     *                       never {@code null}
     * @param job            the task to run; never {@code null}
     * @throws IllegalArgumentException if {@code name} is already registered or
     *                                  the cron expression is malformed
     */
    public void schedule(String name, String cronExpression, TimerJob job) {
        Objects.requireNonNull(name,           "name");
        Objects.requireNonNull(cronExpression, "cronExpression");
        Objects.requireNonNull(job,            "job");
        checkNotRegistered(name);

        CronExpression cron = CronExpression.parse(cronExpression);
        JobEntry entry = new JobEntry(job, cron, null, null, null, JobStatus.RUNNING);
        jobs.put(name, entry);
        scheduleCronRun(name, cron, job);
    }

    /**
     * Schedules a job to run <em>once</em> at the given instant. If the instant is
     * already in the past the job runs as soon as possible.
     *
     * <p>After it fires, the job's status becomes {@link JobStatus#COMPLETED}. A
     * pending one-shot can be {@link #suspend(String) suspended} (which disarms
     * it) and {@link #resume(String) resumed} (which re-arms it for the original
     * instant — immediately, if that instant has since passed) or
     * {@link #cancel(String) cancelled}.
     *
     * <p>This schedule lives only in memory: a one-shot pending when the JVM
     * stops is lost. For deadlines that must survive a restart, persist them in
     * your own table and rearm on startup.
     *
     * @param name a unique job name; never {@code null}
     * @param when the instant to run at; never {@code null}
     * @param job  the task to run; never {@code null}
     * @throws IllegalArgumentException if {@code name} is already registered
     */
    public void schedule(String name, Instant when, TimerJob job) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(when, "when");
        Objects.requireNonNull(job,  "job");
        checkNotRegistered(name);

        jobs.put(name, new JobEntry(job, null, null, when, null, JobStatus.RUNNING));
        scheduleOneShotRun(name, when, job);
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /**
     * Suspends a running job. The current execution (if any) completes normally;
     * no further executions are scheduled until {@link #resume(String)}.
     *
     * @param name the job name; never {@code null}
     * @throws IllegalArgumentException if the job is not registered
     */
    public void suspend(String name) {
        JobEntry entry = requireEntry(name);
        if (entry.status() == JobStatus.RUNNING) {
            if (entry.future() != null) entry.future().cancel(false);
            jobs.put(name, entry.withStatus(JobStatus.SUSPENDED));
            log.info("Job '{}' suspended", name);
        }
    }

    /**
     * Resumes a suspended job.
     *
     * <p>For interval jobs, the next run is scheduled after the original interval.
     * For cron jobs, the next scheduled time is recalculated from now.
     *
     * @param name the job name; never {@code null}
     * @throws IllegalArgumentException if the job is not registered
     * @throws IllegalStateException    if the job is not suspended
     */
    public void resume(String name) {
        JobEntry entry = requireEntry(name);
        if (entry.status() != JobStatus.SUSPENDED) {
            throw new IllegalStateException("Job '" + name + "' is not suspended (status: " + entry.status() + ")");
        }
        if (entry.cron() != null) {
            jobs.put(name, entry.withStatus(JobStatus.RUNNING).withFuture(null));
            scheduleCronRun(name, entry.cron(), entry.job());
        } else if (entry.interval() != null) {
            long millis = entry.interval().toMillis();
            Future<?> future = executor.scheduleAtFixedRate(
                () -> runJob(name, entry.job()), millis, millis, TimeUnit.MILLISECONDS);
            jobs.put(name, entry.withStatus(JobStatus.RUNNING).withFuture(future));
        } else {
            jobs.put(name, entry.withStatus(JobStatus.RUNNING).withFuture(null));
            scheduleOneShotRun(name, entry.fireAt(), entry.job());
        }
        log.info("Job '{}' resumed", name);
    }

    /**
     * Permanently cancels a job. Cannot be resumed after cancellation.
     *
     * @param name the job name; never {@code null}
     * @throws IllegalArgumentException if the job is not registered
     */
    public void cancel(String name) {
        JobEntry entry = requireEntry(name);
        if (entry.future() != null) entry.future().cancel(false);
        jobs.put(name, entry.withStatus(JobStatus.CANCELLED));
        log.info("Job '{}' cancelled", name);
    }

    /**
     * Returns the current status of a job.
     *
     * @param name the job name; never {@code null}
     * @return the job's status
     * @throws IllegalArgumentException if the job is not registered
     */
    public JobStatus status(String name) {
        return requireEntry(name).status();
    }

    /**
     * Returns a snapshot of every registered job and its current status
     * (for monitoring).
     *
     * @return an immutable map of job name to status
     */
    public Map<String, JobStatus> jobStatuses() {
        var snapshot = new java.util.LinkedHashMap<String, JobStatus>();
        jobs.forEach((name, entry) -> snapshot.put(name, entry.status()));
        return java.util.Collections.unmodifiableMap(snapshot);
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private void runJob(String name, TimerJob job) {
        JobEntry entry = jobs.get(name);
        if (entry == null || entry.status() != JobStatus.RUNNING) return;
        try {
            // Each run is a system-principal unit of work: the job's repository
            // operations commit atomically and its published events fire after.
            runner.run(ctx -> { job.run(ctx); return null; }, Principal.system(), locale);
        } catch (AppServiceException e) {
            log.error("Timer job '{}' threw", name, e);
        }
    }

    private void scheduleCronRun(String name, CronExpression cron, TimerJob job) {
        ZonedDateTime next  = cron.nextAfter(ZonedDateTime.now());
        long delayMillis    = Duration.between(ZonedDateTime.now(), next).toMillis();
        if (delayMillis < 0) delayMillis = 0;

        Future<?> future = executor.schedule(() -> {
            runJob(name, job);
            JobEntry current = jobs.get(name);
            if (current != null && current.status() == JobStatus.RUNNING) {
                scheduleCronRun(name, cron, job);
            }
        }, delayMillis, TimeUnit.MILLISECONDS);

        jobs.computeIfPresent(name, (k, e) -> e.withFuture(future));
    }

    private void scheduleOneShotRun(String name, Instant when, TimerJob job) {
        long delayMillis = Duration.between(Instant.now(), when).toMillis();
        if (delayMillis < 0) delayMillis = 0;

        Future<?> future = executor.schedule(() -> {
            runJob(name, job);
            // One-shot: a successful fire moves RUNNING -> COMPLETED. Leave a
            // concurrently suspended/cancelled job in its terminal/paused state.
            jobs.computeIfPresent(name, (k, e) ->
                e.status() == JobStatus.RUNNING ? e.withStatus(JobStatus.COMPLETED) : e);
        }, delayMillis, TimeUnit.MILLISECONDS);

        jobs.computeIfPresent(name, (k, e) -> e.withFuture(future));
    }

    private void checkNotRegistered(String name) {
        if (jobs.containsKey(name)) {
            throw new IllegalArgumentException("Job already registered: " + name);
        }
    }

    private JobEntry requireEntry(String name) {
        JobEntry entry = jobs.get(Objects.requireNonNull(name, "name"));
        if (entry == null) {
            throw new IllegalArgumentException("Unknown job: " + name);
        }
        return entry;
    }

    // ── Job entry (internal state) ────────────────────────────────────────────

    private record JobEntry(
            TimerJob job,
            CronExpression cron,     // null unless a cron job
            Duration interval,       // null unless an interval job
            Instant fireAt,          // null unless a one-shot job
            Future<?> future,
            JobStatus status) {

        JobEntry withStatus(JobStatus s) {
            return new JobEntry(job, cron, interval, fireAt, future, s);
        }

        JobEntry withFuture(Future<?> f) {
            return new JobEntry(job, cron, interval, fireAt, f, status);
        }
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    /**
     * Builder for {@link TimerScheduler}.
     */
    public static final class Builder {

        private DataSource dataSource;
        private Locale locale = Locale.getDefault();
        private int poolSize = DEFAULT_POOL_SIZE;

        private Builder() {}

        /**
         * Sets the number of scheduler threads, i.e. how many jobs may run
         * concurrently. Defaults to {@link #DEFAULT_POOL_SIZE}.
         *
         * @param poolSize the thread count; must be at least 1
         * @return this builder
         */
        public Builder poolSize(int poolSize) {
            if (poolSize < 1) {
                throw new IllegalArgumentException("poolSize must be >= 1: " + poolSize);
            }
            this.poolSize = poolSize;
            return this;
        }

        /**
         * Sets the data source used when creating {@link AppContext} for job runs.
         *
         * @param dataSource the JDBC data source; never {@code null}
         * @return this builder
         */
        public Builder dataSource(DataSource dataSource) {
            this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
            return this;
        }

        /**
         * Sets the locale injected into the system {@link AppContext} for each job run.
         *
         * @param locale the locale; never {@code null}
         * @return this builder
         */
        public Builder locale(Locale locale) {
            this.locale = Objects.requireNonNull(locale, "locale");
            return this;
        }

        /**
         * Builds the {@link TimerScheduler}.
         *
         * @return a new scheduler
         * @throws IllegalStateException if no data source was provided
         */
        public TimerScheduler build() {
            if (dataSource == null) {
                throw new IllegalStateException("dataSource must be set");
            }
            return new TimerScheduler(dataSource, locale, poolSize);
        }
    }
}
