package org.jusnga.pageviews;

import org.jusnga.pageviews.aggregators.topviews.TopPageViews;

import java.util.Map;

/**
 * Kept things super simple here, reality is that you would want to query an actual time range and get all date and hours
 * between.
 */
public interface PageViewsService extends AutoCloseable {
    TopPageViews getTopPageViews(DateAndHour dateAndHour);

    Map<DateAndHour, TopPageViews> getTopPageViews(DateAndHour from, DateAndHour to);
}
