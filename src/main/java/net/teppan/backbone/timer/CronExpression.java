package net.teppan.backbone.timer;

import java.time.DayOfWeek;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Parses and evaluates a 6-field cron expression.
 *
 * <p>Field order: {@code second  minute  hour  dayOfMonth  month  dayOfWeek}
 *
 * <table class="striped">
 * <caption>Field ranges</caption>
 * <thead><tr><th>Field</th><th>Range</th><th>Special values</th></tr></thead>
 * <tbody>
 * <tr><td>second</td><td>0–59</td><td></td></tr>
 * <tr><td>minute</td><td>0–59</td><td></td></tr>
 * <tr><td>hour</td><td>0–23</td><td></td></tr>
 * <tr><td>dayOfMonth</td><td>1–31</td><td></td></tr>
 * <tr><td>month</td><td>1–12</td><td>JAN–DEC</td></tr>
 * <tr><td>dayOfWeek</td><td>0–6</td><td>SUN–SAT (0=Sunday)</td></tr>
 * </tbody>
 * </table>
 *
 * <p>Supported syntax per field:
 * <ul>
 *   <li>{@code *} — every value</li>
 *   <li>{@code n} — specific value</li>
 *   <li>{@code n,m} — list (elements may themselves be ranges)</li>
 *   <li>{@code n-m} — inclusive range</li>
 *   <li>{@code *&#47;s} — step from minimum</li>
 *   <li>{@code n-m/s} — range with step</li>
 * </ul>
 *
 * <p>Both dayOfMonth and dayOfWeek must match (AND semantics).
 *
 * <pre>{@code
 * CronExpression every5min = CronExpression.parse("0 *&#47;5 * * * *");
 * ZonedDateTime next = every5min.nextAfter(ZonedDateTime.now());
 * }</pre>
 */
public final class CronExpression {

    private static final int MAX_YEARS_AHEAD = 4;

    private static final Map<String, Integer> MONTH_NAMES = Map.ofEntries(
        Map.entry("JAN", 1),  Map.entry("FEB", 2),  Map.entry("MAR", 3),
        Map.entry("APR", 4),  Map.entry("MAY", 5),  Map.entry("JUN", 6),
        Map.entry("JUL", 7),  Map.entry("AUG", 8),  Map.entry("SEP", 9),
        Map.entry("OCT", 10), Map.entry("NOV", 11), Map.entry("DEC", 12));

    private static final Map<String, Integer> DOW_NAMES = Map.of(
        "SUN", 0, "MON", 1, "TUE", 2, "WED", 3,
        "THU", 4, "FRI", 5, "SAT", 6);

    private final int[] seconds;
    private final int[] minutes;
    private final int[] hours;
    private final int[] daysOfMonth;
    private final int[] months;
    private final int[] daysOfWeek;

    private CronExpression(int[] seconds, int[] minutes, int[] hours,
                           int[] daysOfMonth, int[] months, int[] daysOfWeek) {
        this.seconds     = seconds;
        this.minutes     = minutes;
        this.hours       = hours;
        this.daysOfMonth = daysOfMonth;
        this.months      = months;
        this.daysOfWeek  = daysOfWeek;
    }

    /**
     * Parses a 6-field cron expression.
     *
     * @param expression the cron string; never {@code null}
     * @return the parsed expression
     * @throws IllegalArgumentException if the expression is malformed
     */
    public static CronExpression parse(String expression) {
        String[] parts = expression.trim().split("\\s+");
        if (parts.length != 6) {
            throw new IllegalArgumentException(
                "Cron expression must have exactly 6 fields, got "
                + parts.length + ": \"" + expression + "\"");
        }
        return new CronExpression(
            parseField(parts[0], 0, 59, null),
            parseField(parts[1], 0, 59, null),
            parseField(parts[2], 0, 23, null),
            parseField(parts[3], 1, 31, null),
            parseField(parts[4], 1, 12, MONTH_NAMES),
            parseField(parts[5], 0, 6,  DOW_NAMES));
    }

    /**
     * Returns the next trigger time strictly after {@code from}.
     *
     * @param from the reference time; never {@code null}
     * @return the next trigger time
     * @throws IllegalStateException if no trigger time exists within
     *                               {@value #MAX_YEARS_AHEAD} years
     */
    public ZonedDateTime nextAfter(ZonedDateTime from) {
        ZonedDateTime candidate = from.plusSeconds(1).truncatedTo(ChronoUnit.SECONDS);
        ZonedDateTime limit     = from.plusYears(MAX_YEARS_AHEAD);

        while (candidate.isBefore(limit)) {

            // ── Month ─────────────────────────────────────────────────────────
            int month = candidate.getMonthValue();
            int nm    = nextAtOrAfter(months, month);
            if (nm < 0) {
                candidate = candidate.plusYears(1)
                    .withMonth(months[0]).withDayOfMonth(1).with(LocalTime.MIDNIGHT);
                continue;
            }
            if (nm != month) {
                candidate = candidate.withMonth(nm).withDayOfMonth(1).with(LocalTime.MIDNIGHT);
                continue;
            }

            // ── Day ───────────────────────────────────────────────────────────
            int day = candidate.getDayOfMonth();
            int dow = toDow(candidate.getDayOfWeek());  // 0=Sunday
            if (!contains(daysOfMonth, day) || !contains(daysOfWeek, dow)) {
                ZonedDateTime nextDay = findNextValidDay(candidate);
                if (nextDay == null) {
                    // No valid day left in this month
                    candidate = candidate.plusMonths(1).withDayOfMonth(1).with(LocalTime.MIDNIGHT);
                } else {
                    candidate = nextDay;
                }
                continue;
            }

            // ── Hour ──────────────────────────────────────────────────────────
            int hour = candidate.getHour();
            int nh   = nextAtOrAfter(hours, hour);
            if (nh < 0) {
                candidate = candidate.plusDays(1).with(LocalTime.MIDNIGHT);
                continue;
            }
            if (nh != hour) {
                candidate = candidate.withHour(nh).withMinute(minutes[0]).withSecond(seconds[0]);
                continue;
            }

            // ── Minute ────────────────────────────────────────────────────────
            int minute = candidate.getMinute();
            int nmin   = nextAtOrAfter(minutes, minute);
            if (nmin < 0) {
                candidate = candidate.plusHours(1).withMinute(minutes[0]).withSecond(seconds[0]);
                continue;
            }
            if (nmin != minute) {
                candidate = candidate.withMinute(nmin).withSecond(seconds[0]);
                continue;
            }

            // ── Second ────────────────────────────────────────────────────────
            int second = candidate.getSecond();
            int nsec   = nextAtOrAfter(seconds, second);
            if (nsec < 0) {
                candidate = candidate.plusMinutes(1).withSecond(seconds[0]);
                continue;
            }
            if (nsec != second) {
                candidate = candidate.withSecond(nsec);
                continue;
            }

            return candidate;
        }
        throw new IllegalStateException(
            "No trigger found within " + MAX_YEARS_AHEAD + " years for: " + this);
    }

    @Override
    public String toString() {
        return "CronExpression{"
            + "s=" + Arrays.toString(seconds)
            + " m=" + Arrays.toString(minutes)
            + " h=" + Arrays.toString(hours)
            + " dom=" + Arrays.toString(daysOfMonth)
            + " mo=" + Arrays.toString(months)
            + " dow=" + Arrays.toString(daysOfWeek) + "}";
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Scan forward day-by-day within the same month for a valid day. */
    private ZonedDateTime findNextValidDay(ZonedDateTime start) {
        ZonedDateTime day = start.plusDays(1).with(LocalTime.MIDNIGHT);
        int startMonth    = start.getMonthValue();
        for (int i = 0; i < 32; i++) {
            if (day.getMonthValue() != startMonth) return null;
            if (contains(daysOfMonth, day.getDayOfMonth())
                && contains(daysOfWeek, toDow(day.getDayOfWeek()))) {
                return day;
            }
            day = day.plusDays(1);
        }
        return null;
    }

    private static int toDow(DayOfWeek dow) {
        // Java: MONDAY=1 … SUNDAY=7 → our 0=Sunday 1=Monday … 6=Saturday
        return dow.getValue() % 7;
    }

    private static int nextAtOrAfter(int[] sorted, int value) {
        for (int v : sorted) {
            if (v >= value) return v;
        }
        return -1;
    }

    private static boolean contains(int[] sorted, int value) {
        for (int v : sorted) {
            if (v == value) return true;
        }
        return false;
    }

    // ── Parsing ───────────────────────────────────────────────────────────────

    private static int[] parseField(String field, int min, int max,
                                    Map<String, Integer> names) {
        TreeSet<Integer> values = new TreeSet<>();
        for (String part : field.split(",")) {
            parseSegment(part.trim(), min, max, names, values);
        }
        if (values.isEmpty()) {
            throw new IllegalArgumentException("Field produced no values: \"" + field + "\"");
        }
        return values.stream().mapToInt(Integer::intValue).toArray();
    }

    private static void parseSegment(String seg, int min, int max,
                                     Map<String, Integer> names, TreeSet<Integer> out) {
        // Handle step: "X/S" or "*/S" or "A-B/S"
        int step = 1;
        int slashIdx = seg.indexOf('/');
        if (slashIdx >= 0) {
            step = Integer.parseInt(seg.substring(slashIdx + 1).trim());
            if (step < 1) {
                throw new IllegalArgumentException("Step must be >= 1: \"" + seg + "\"");
            }
            seg  = seg.substring(0, slashIdx).trim();
        }

        int rangeStart, rangeEnd;
        if ("*".equals(seg)) {
            rangeStart = min;
            rangeEnd   = max;
        } else {
            int dashIdx = seg.indexOf('-');
            if (dashIdx >= 0) {
                rangeStart = resolveValue(seg.substring(0, dashIdx).trim(), names);
                rangeEnd   = resolveValue(seg.substring(dashIdx + 1).trim(), names);
            } else {
                rangeStart = resolveValue(seg, names);
                rangeEnd   = (slashIdx >= 0) ? max : rangeStart;
            }
        }

        if (rangeStart < min || rangeEnd > max || rangeStart > rangeEnd) {
            throw new IllegalArgumentException(
                "Invalid range " + rangeStart + "-" + rangeEnd
                + " for bounds [" + min + "," + max + "]");
        }
        for (int v = rangeStart; v <= rangeEnd; v += step) {
            out.add(v);
        }
    }

    private static int resolveValue(String token, Map<String, Integer> names) {
        if (names != null) {
            Integer named = names.get(token.toUpperCase());
            if (named != null) return named;
        }
        try {
            return Integer.parseInt(token);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid cron value: \"" + token + "\"");
        }
    }
}
