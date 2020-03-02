package org.jusnga.pageviews.sources;

import org.jusnga.pageviews.DateAndHour;

import java.util.List;
import java.util.Map;

public interface PageViewsSource extends AutoCloseable {
    Map<DateAndHour, PageViewsResource> getPageViewsResource(List<DateAndHour> dateAndHours);
}