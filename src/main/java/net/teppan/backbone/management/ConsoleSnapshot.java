package net.teppan.backbone.management;

import net.teppan.backbone.timer.TimerScheduler.JobStatus;

import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/**
 * A point-in-time view of a running backbone, gathered by
 * {@link BackboneConsole#snapshot()} for monitoring dashboards and operational
 * tooling.
 *
 * @param services           the names of the registered services
 * @param pendingEvents      durable events awaiting delivery, or empty when
 *                           durable events are not enabled
 * @param deadLetteredEvents durable events that failed permanently, or empty
 *                           when durable events are not enabled
 * @param jobs               each scheduled job's name and current status; empty
 *                           when no scheduler is attached
 */
public record ConsoleSnapshot(Set<String> services,
                              OptionalLong pendingEvents,
                              OptionalLong deadLetteredEvents,
                              Map<String, JobStatus> jobs) {
}
