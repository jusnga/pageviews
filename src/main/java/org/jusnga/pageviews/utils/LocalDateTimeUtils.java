package org.jusnga.pageviews.utils;

import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.jusnga.pageviews.DateAndHour;

public final class LocalDateTimeUtils {
    private LocalDateTimeUtils() {}

    public static LocalDateTime toDateTime(DateAndHour dateAndHour) {
        LocalDate date = dateAndHour.getDate();
        int hour = dateAndHour.getHour();

        return date.toLocalDateTime(LocalTime.MIDNIGHT).plusHours(hour);
    }
}
