package org.jusnga.pageviews.io;

import org.jusnga.pageviews.DateAndHour;
import org.jusnga.pageviews.PageViews;

import java.util.List;

public interface PageViewsSource {
    List<PageViews> getPageViews(DateAndHour dateAndHour);
}