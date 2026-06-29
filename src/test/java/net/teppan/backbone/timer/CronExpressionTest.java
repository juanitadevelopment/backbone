package net.teppan.backbone.timer;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CronExpressionTest {

    private static ZonedDateTime at(int year, int month, int day, int hour, int min, int sec) {
        return ZonedDateTime.of(year, month, day, hour, min, sec, 0, ZoneId.systemDefault());
    }

    @Test
    void everySecond_nextIsOneTick() {
        var cron = CronExpression.parse("* * * * * *");
        var from = at(2026, 1, 1, 0, 0, 0);
        assertThat(cron.nextAfter(from)).isEqualTo(at(2026, 1, 1, 0, 0, 1));
    }

    @Test
    void specificTime_findsNextOccurrence() {
        // 0 0 2 * * * = every day at 02:00:00
        var cron = CronExpression.parse("0 0 2 * * *");
        var from = at(2026, 6, 1, 0, 0, 0);
        assertThat(cron.nextAfter(from)).isEqualTo(at(2026, 6, 1, 2, 0, 0));
    }

    @Test
    void specificTime_whenPast_advancesToNextDay() {
        var cron = CronExpression.parse("0 0 2 * * *");
        var from = at(2026, 6, 1, 3, 0, 0);
        assertThat(cron.nextAfter(from)).isEqualTo(at(2026, 6, 2, 2, 0, 0));
    }

    @Test
    void everyFiveMinutes_nextIsCorrect() {
        // 0 */5 * * * *
        var cron = CronExpression.parse("0 */5 * * * *");
        var from = at(2026, 1, 1, 10, 3, 0);
        assertThat(cron.nextAfter(from)).isEqualTo(at(2026, 1, 1, 10, 5, 0));
    }

    @Test
    void everyFiveMinutes_atBoundary_nextIsNextBoundary() {
        var cron = CronExpression.parse("0 */5 * * * *");
        var from = at(2026, 1, 1, 10, 5, 0);
        // from is exactly on the boundary, nextAfter is strictly after
        assertThat(cron.nextAfter(from)).isEqualTo(at(2026, 1, 1, 10, 10, 0));
    }

    @Test
    void range_secondsInRange() {
        // 10-15 * * * * *
        var cron = CronExpression.parse("10-15 * * * * *");
        var from = at(2026, 1, 1, 0, 0, 9);
        assertThat(cron.nextAfter(from)).isEqualTo(at(2026, 1, 1, 0, 0, 10));
    }

    @Test
    void range_wrapsToNextMinute() {
        var cron = CronExpression.parse("10-15 * * * * *");
        var from = at(2026, 1, 1, 0, 0, 16);
        assertThat(cron.nextAfter(from)).isEqualTo(at(2026, 1, 1, 0, 1, 10));
    }

    @Test
    void listField_picksNextInList() {
        // 0 0 6,12,18 * * *
        var cron = CronExpression.parse("0 0 6,12,18 * * *");
        var from = at(2026, 1, 1, 7, 0, 0);
        assertThat(cron.nextAfter(from)).isEqualTo(at(2026, 1, 1, 12, 0, 0));
    }

    @Test
    void monthName_parsedCorrectly() {
        // 0 0 0 1 JAN *
        var cron = CronExpression.parse("0 0 0 1 JAN *");
        var from = at(2026, 2, 1, 0, 0, 0);
        assertThat(cron.nextAfter(from)).isEqualTo(at(2027, 1, 1, 0, 0, 0));
    }

    @Test
    void dayOfWeek_sunday_zero() {
        // 0 0 9 * * 0  = every Sunday at 09:00
        // 2026-06-28 is a Sunday
        var cron = CronExpression.parse("0 0 9 * * 0");
        var from = at(2026, 6, 27, 0, 0, 0); // Saturday
        var next = cron.nextAfter(from);
        assertThat(next).isEqualTo(at(2026, 6, 28, 9, 0, 0));
    }

    @Test
    void dayOfWeekName_sun() {
        var cron = CronExpression.parse("0 0 9 * * SUN");
        var from = at(2026, 6, 27, 0, 0, 0);
        assertThat(cron.nextAfter(from)).isEqualTo(at(2026, 6, 28, 9, 0, 0));
    }

    @Test
    void stepWithRange() {
        // 0 0 8-20/2 * * * = 8,10,12,14,16,18,20
        var cron = CronExpression.parse("0 0 8-20/2 * * *");
        var from = at(2026, 1, 1, 9, 0, 0);
        assertThat(cron.nextAfter(from)).isEqualTo(at(2026, 1, 1, 10, 0, 0));
    }

    @Test
    void invalidFieldCount_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> CronExpression.parse("* * * * *"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("6 fields");
    }

    @Test
    void invalidValue_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> CronExpression.parse("60 * * * * *"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void zeroStep_throwsInsteadOfHanging() {
        assertThatThrownBy(() -> CronExpression.parse("*/0 * * * * *"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Step");
    }
}
