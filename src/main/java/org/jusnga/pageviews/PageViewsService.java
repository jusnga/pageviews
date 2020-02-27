package org.jusnga.pageviews;

import java.util.Map;

public interface PageViewsService {
    TopPageViews getTopPageViews(DateAndHour dateAndHour);

    Map<DateAndHour, TopPageViews> getTopPageViews(DateAndHour from, DateAndHour to);
}
