package org.jusnga.pageviews;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.joda.time.LocalDate;
import org.jusnga.pageviews.cache.ResultCache;
import org.jusnga.pageviews.cache.filesystem.TopViewsFSCache;
import org.jusnga.pageviews.sources.PageViewsResource;
import org.jusnga.pageviews.sources.PageViewsSource;
import org.jusnga.pageviews.sources.dumpswikimedia.WikimediaPageViewsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Idea here is that the downloading of the files should be decoupled from the processing of the files, this should open
 * up capabilities for more streamlined processing. I'd ideally model this as a multi-stage pipeline to
 * decouple the downloading/processing (i.e. once a file has finished downloading,
 * you begin processing as opposed to waiting for every other file to download), not too familiar with flink but I believe they have
 * some notion of operation chaining that captures this model. Being a toy problem it's hard to justify doing it this way.
 */
public class DefaultPageViewsService implements PageViewsService {
    private final PageViewsSource pageViewsSource;
    private final PageViewsProcessor pageViewsProcessor;
    private final ResultCache<DateAndHour, TopPageViews> topPageViewsCache;

    private static final int NUM_PARALLEL_DOWNLOADS = 3;
    private static final int NUM_PARALLEL_PROCESSES = 8;

    private static final Logger logger = LoggerFactory.getLogger(DefaultPageViewsService.class);

    public DefaultPageViewsService(Path downloadPath, Path cachePath) throws IOException {
        this.pageViewsSource = new WikimediaPageViewsSource(downloadPath, NUM_PARALLEL_DOWNLOADS);
        this.pageViewsProcessor = new PageViewsProcessor(NUM_PARALLEL_PROCESSES);
        this.topPageViewsCache = new TopViewsFSCache(cachePath);
    }

    @Override
    public TopPageViews getTopPageViews(DateAndHour dateAndHour) {
        return Iterables.getOnlyElement(getTopPageViews(dateAndHour, dateAndHour).values());
    }

    @Override
    public Map<DateAndHour, TopPageViews> getTopPageViews(DateAndHour from, DateAndHour to) {
        List<DateAndHour> dateAndHours = DateAndHour.getDateAndHours(from, to);

        Map<DateAndHour, TopPageViews> results = Maps.newHashMap();

        Iterator<DateAndHour> it = dateAndHours.iterator();
        while (it.hasNext()) {
            DateAndHour next = it.next();

            Optional<TopPageViews> maybeCachedTopPageViews = topPageViewsCache.getCachedResult(next);
            if (maybeCachedTopPageViews.isPresent()) {
                logger.info("Found cached result for {}", next);
                results.put(next, maybeCachedTopPageViews.get());
                it.remove();
            }
        }

        Map<DateAndHour, PageViewsResource> pageViewsResources = pageViewsSource.getPageViewsResource(dateAndHours);

        long start = System.currentTimeMillis();
        Map<DateAndHour, TopPageViews> processedTopViews = pageViewsProcessor.getTopPageViews(pageViewsResources);
        long timeTaken = System.currentTimeMillis() - start;
        logger.info("Processed {} resources in {} ms", results.size(), timeTaken);

        processedTopViews.forEach((dateAndHour, topViews) -> {
            topPageViewsCache.cacheResult(dateAndHour, topViews);
            results.put(dateAndHour, topViews);
        });

        return results;
    }

    @Override
    public void close() throws Exception {
        pageViewsSource.close();
        pageViewsProcessor.close();
    }
}
