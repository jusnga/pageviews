package org.jusnga.pageviews;

import com.google.common.collect.Maps;
import org.jusnga.pageviews.cache.ResultCache;
import org.jusnga.pageviews.cache.filesystem.TopViewsFSCache;
import org.jusnga.pageviews.processors.TopViewsProcessor;
import org.jusnga.pageviews.sources.PageViewsSource;
import org.jusnga.pageviews.sources.dumpswikimedia.WikimediaPageViewsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DefaultPageViewsService implements PageViewsService {
    private final PageViewsSource pageViewsSource;
    private final ResultCache<DateAndHour, TopPageViews> topPageViewsCache;

    private static final int NUM_TOP_VIEWS = 25;

    private static final Logger logger = LoggerFactory.getLogger(DefaultPageViewsService.class);

    public DefaultPageViewsService(Path downloadPath, Path cachePath) throws IOException {
        this.pageViewsSource = new WikimediaPageViewsSource(downloadPath);
        this.topPageViewsCache = new TopViewsFSCache(cachePath);
    }

    @Override
    public TopPageViews getTopPageViews(DateAndHour dateAndHour) {
        Optional<TopPageViews> maybeCachedTopPageViews = topPageViewsCache.getCachedResult(dateAndHour);
        if (maybeCachedTopPageViews.isPresent()) {
            logger.info("Found cached result for {}", dateAndHour);
            return maybeCachedTopPageViews.get();
        }

        List<PageViews> allPageViews = pageViewsSource.getPageViews(dateAndHour);

        TopViewsProcessor topViewsProcessor = new TopViewsProcessor(NUM_TOP_VIEWS);
        allPageViews.forEach(topViewsProcessor::process);

        TopPageViews topPageViews = TopPageViews.from(topViewsProcessor.getTopPageViews());
        topPageViewsCache.cacheResult(dateAndHour, topPageViews);

        return topPageViews;
    }

    @Override
    public Map<DateAndHour, TopPageViews> getTopPageViews(DateAndHour from, DateAndHour to) {
        List<DateAndHour> dateAndHours = DateAndHour.getDateAndHours(from, to);

        Map<DateAndHour, TopPageViews> results = Maps.newHashMap();
        for (DateAndHour dateAndHour: dateAndHours) {
            TopPageViews topPageViews = getTopPageViews(dateAndHour);

            results.put(dateAndHour, topPageViews);
        }

        return results;
    }
}
