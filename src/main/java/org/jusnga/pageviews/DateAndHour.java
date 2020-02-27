package org.jusnga.pageviews;

import org.joda.time.Hours;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    public DateAndHour(LocalDateTime dateTime) {
        this.date = dateTime.toLocalDate();
        this.hour = dateTime.getHourOfDay();
    }

    public LocalDate getDate() {
        return date;
    }

    public int getYear() {
        return date.getYear();
    }

    public int getMonth() {
        return date.getMonthOfYear();
    }

    @Override
    public String toString() {
        return "DateAndHour{" +
                "date=" + date +
                ", hour=" + hour +
                '}';
    }

    public LocalDateTime getLocalDateTime() {
        return date.toLocalDateTime(LocalTime.MIDNIGHT).plusHours(hour);
    }

    public static List<DateAndHour> getDateAndHours(DateAndHour from, DateAndHour to) {
        LocalDateTime fromDateTime = from.getLocalDateTime();
        LocalDateTime toDateTime = to.getLocalDateTime();

        int hoursBetween = Hours.hoursBetween(fromDateTime, toDateTime).getHours();

        return IntStream.range(0, hoursBetween + 1)
                .mapToObj(fromDateTime::plusHours)
                .map(DateAndHour::new)
                .collect(Collectors.toList());
    }
}
