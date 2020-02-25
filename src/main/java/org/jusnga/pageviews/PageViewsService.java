package org.jusnga.pageviews;

import java.util.List;

public interface PageViewsService {
    List<PageViews> getTopPageViews(DateAndHour dateAndHour);
}
