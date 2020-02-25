package org.jusnga.pageviews;

import org.joda.time.LocalDate;

public final class DateAndHour {
    private final LocalDate date;
    private final int hour;

    public DateAndHour(LocalDate date, int hour) {
        if (hour < 0 || hour > 23) {
            throw new IllegalArgumentException("Hour must be between 0 and 23");
        }
        this.hour = hour;
        this.date = date;
    }

    public int getHour() {
        return hour;
    }

    public LocalDate getDate() {
        return date;
    }

    public static DateAndHour from(LocalDate date, int hour) {
        return new DateAndHour(date, hour);
    }
}
