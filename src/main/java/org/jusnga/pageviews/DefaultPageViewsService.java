package org.jusnga.pageviews;

import org.joda.time.LocalDate;
import org.jusnga.pageviews.io.PageViewsSource;
import org.jusnga.pageviews.io.dumpswikimedia.WikimediaPageViewsSource;
import org.jusnga.pageviews.processors.TopViewsProcessor;

import java.util.List;

public class DefaultPageViewsService implements PageViewsService {
    private final PageViewsSource pageViewsSource = new WikimediaPageViewsSource();

    private static final int NUM_TOP_VIEWS = 25;

    @Override
    public List<PageViews> getTopPageViews(DateAndHour dateAndHour) {
        List<PageViews> allPageViews = pageViewsSource.getPageViews(dateAndHour);

        TopViewsProcessor topViewsProcessor = new TopViewsProcessor(NUM_TOP_VIEWS);

        allPageViews.forEach(topViewsProcessor::process);

        return topViewsProcessor.getTopPageViews();
    }

    public static void main(String[] args) {
        PageViewsService pageViewsService = new DefaultPageViewsService();

        List<PageViews> topPageViews = pageViewsService.getTopPageViews(
                DateAndHour.from(
                        LocalDate.now().minusYears(3),
                        10
                )
        );

        System.out.println(topPageViews);
    }
}
