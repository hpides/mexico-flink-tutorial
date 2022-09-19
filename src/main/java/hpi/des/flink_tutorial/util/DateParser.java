package hpi.des.flink_tutorial.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class DateParser {
    public static long localDateTimeToMilliseconds(LocalDateTime dateTime) {
        ZonedDateTime zone = dateTime.atZone(ZoneId.of("America/New_York"));
        return zone.toInstant().toEpochMilli();
    }

    public static LocalDateTime millisecondsToLocalDateTime(long timeInMs){
        Instant instant = Instant.ofEpochMilli(timeInMs);
        return instant.atZone(ZoneId.of("America/New_York")).toLocalDateTime();
    }
}