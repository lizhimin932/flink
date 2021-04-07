package org.apache.flink.table.runtime.util;

import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.TimeZone;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.toEpochMills;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toEpochMillsForTimer;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills;
import static org.junit.Assert.assertEquals;

/** Test for {@link org.apache.flink.table.runtime.util.TimeWindowUtil}. */
public class TimeWindowUtilTest {

    private static final ZoneId UTC_ZONE_ID = TimeZone.getTimeZone("UTC").toZoneId();

    @Test
    public void testShiftedTimeZone() {
        ZoneId zoneId = ZoneId.of("Asia/Shanghai");
        assertEquals(-28799000L, toEpochMillsForTimer(utcMills("1970-01-01T00:00:01"), zoneId));
        assertEquals(-1L, toEpochMillsForTimer(utcMills("1970-01-01T07:59:59.999"), zoneId));
        assertEquals(1000L, toEpochMillsForTimer(utcMills("1970-01-01T08:00:01"), zoneId));
        assertEquals(1L, toEpochMillsForTimer(utcMills("1970-01-01T08:00:00.001"), zoneId));
    }

    @Test
    public void testDaylightSaving() {
        ZoneId zoneId = ZoneId.of("America/Los_Angeles");
        /*
         * The DaylightTime in Los_Angele start at time 2021-03-14 02:00:00
         * <pre>
         *  2021-03-14 00:00:00 -> epoch1 = 1615708800000L;
         *  2021-03-14 01:00:00 -> epoch2 = 1615712400000L;
         *  2021-03-14 03:00:00 -> epoch3 = 1615716000000L;  skip one hour (2021-03-14 02:00:00)
         *  2021-03-14 04:00:00 -> epoch4 = 1615719600000L;
         */
        assertEquals(1615708800000L, toEpochMillsForTimer(utcMills("2021-03-14T00:00:00"), zoneId));
        assertEquals(1615712400000L, toEpochMillsForTimer(utcMills("2021-03-14T01:00:00"), zoneId));
        assertEquals(1615716000000L, toEpochMillsForTimer(utcMills("2021-03-14T02:00:00"), zoneId));
        assertEquals(1615716000000L, toEpochMillsForTimer(utcMills("2021-03-14T03:00:00"), zoneId));
        assertEquals(1615717800000L, toEpochMillsForTimer(utcMills("2021-03-14T02:30:00"), zoneId));
        assertEquals(1615719599000L, toEpochMillsForTimer(utcMills("2021-03-14T02:59:59"), zoneId));
        assertEquals(1615717800000L, toEpochMillsForTimer(utcMills("2021-03-14T03:30:00"), zoneId));

        /*
         * The DaylightTime in Los_Angele start at time 2021-11-07 01:00:00
         * <pre>
         *  2021-11-07 00:00:00 -> epoch0 = 1636268400000L;  2021-11-07 00:00:00
         *  2021-11-07 01:00:00 -> epoch1 = 1636272000000L;  the first local timestamp 2021-11-07 01:00:00
         *  2021-11-07 01:00:00 -> epoch2 = 1636275600000L;  back to local timestamp  2021-11-07 01:00:00
         *  2021-11-07 02:00:00 -> epoch3 = 1636279200000L;  2021-11-07 02:00:00
         */
        assertEquals(1636268400000L, toEpochMillsForTimer(utcMills("2021-11-07T00:00:00"), zoneId));
        assertEquals(1636275600000L, toEpochMillsForTimer(utcMills("2021-11-07T01:00:00"), zoneId));
        assertEquals(1636279200000L, toEpochMillsForTimer(utcMills("2021-11-07T02:00:00"), zoneId));
        assertEquals(1636268401000L, toEpochMillsForTimer(utcMills("2021-11-07T00:00:01"), zoneId));
        assertEquals(1636279199000L, toEpochMillsForTimer(utcMills("2021-11-07T01:59:59"), zoneId));
        assertEquals(1636279201000L, toEpochMillsForTimer(utcMills("2021-11-07T02:00:01"), zoneId));
    }

    @Test
    public void testMaxWaterMark() {
        ZoneId zoneId = ZoneId.of("Asia/Shanghai");
        assertEquals(Long.MAX_VALUE, toUtcTimestampMills(Long.MAX_VALUE, zoneId));
        assertEquals(Long.MAX_VALUE, toEpochMillsForTimer(Long.MAX_VALUE, zoneId));
        assertEquals(Long.MAX_VALUE, toEpochMills(Long.MAX_VALUE, zoneId));
    }

    private static long utcMills(String utcDateTime) {
        return LocalDateTime.parse(utcDateTime).atZone(UTC_ZONE_ID).toInstant().toEpochMilli();
    }
}
