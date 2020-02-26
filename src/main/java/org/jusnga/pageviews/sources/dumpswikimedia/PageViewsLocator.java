package org.jusnga.pageviews.sources.dumpswikimedia;

import org.joda.time.LocalDateTime;
import org.jusnga.pageviews.DateAndHour;

import java.net.MalformedURLException;
import java.net.URL;

public final class PageViewsLocator {
    private final LocalDateTime dateTime;

    private static final String PAGE_VIEW_DATE_FMT = "yyyyMMdd-HHmmss";
    private static final String PAGE_VIEW_FILE_FMT = "pageviews-%s.gz";
    private static final String YEAR_MONTH_PATH_FMT = "yyyy-MM";

    private static final String BASE_URL = "https://dumps.wikimedia.org/other/pageviews/";

    public PageViewsLocator(LocalDateTime dateTime) {
        this.dateTime = dateTime;
    }

    public URL getUrl() throws MalformedURLException {
        URL basePath = new URL(BASE_URL);
        URL yearPath = new URL(basePath, dateTime.getYear() + "/");
        URL monthPath = new URL(yearPath, dateTime.toString(YEAR_MONTH_PATH_FMT) + "/");
        return new URL(monthPath, getFileName());
    }

    public LocalDateTime getDateTime() {
        return dateTime;
    }

    public int getYear() {
        return dateTime.getYear();
    }

    public int getMonth() {
        return dateTime.getMonthOfYear();
    }

    public String getFileName() {
        return String.format(PAGE_VIEW_FILE_FMT, dateTime.toString(PAGE_VIEW_DATE_FMT));
    }

    public static PageViewsLocator getLocator(DateAndHour dateAndHour) {
        LocalDateTime dateTime = DateAndHour.toLocalDateTime(dateAndHour);

        return new PageViewsLocator(dateTime);
    }

    @Override
    public String toString() {
        return "PageViewsLocator{" +
                "dateTime=" + dateTime + ", " +
                "fileName=" + getFileName() +
                '}';
    }
}
