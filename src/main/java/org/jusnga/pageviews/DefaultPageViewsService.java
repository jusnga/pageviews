package org.jusnga.pageviews;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.joda.time.LocalDate;
import org.jusnga.pageviews.aggregators.topviews.PageViewsProcessor;
import org.jusnga.pageviews.aggregators.topviews.TopPageViews;
import org.jusnga.pageviews.aggregators.topviews.blacklist.PageViewsBlacklist;
import org.jusnga.pageviews.aggregators.topviews.blacklist.PageViewsBlacklistReader;
import org.jusnga.pageviews.cache.ResultCache;
import org.jusnga.pageviews.cache.filesystem.TopViewsFSCache;
import org.jusnga.pageviews.sources.PageViewsResource;
import org.jusnga.pageviews.sources.PageViewsSource;
import org.jusnga.pageviews.sources.dumpswikimedia.WikimediaPageViewsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Idea here is that the downloading of the files should be decoupled from the processing of the files, this should open
 * up capabilities for more streamlined processing as described in the README section.
 *
 * Could probably implement this as a multi threaded java application but  for a production tool you'd probably want to
 * leverage kafka for the messaging/durability layer and something like
 * spark/flink for the orchestration and auto scaling (if using something like kubernetes).
 *
 * Not too familiar with flink, but I believe it's operation chaining/topology is similar to what I describe above.
 */
public class DefaultPageViewsService implements PageViewsService {
    private final PageViewsSource pageViewsSource;
    private final PageViewsProcessor pageViewsProcessor;
    private final ResultCache<DateAndHour, TopPageViews> topPageViewsCache;

    private static final int NUM_PARALLEL_DOWNLOADS = 3;
    private static final int NUM_PARALLEL_PROCESSES = 8;

    private static final Logger logger = LoggerFactory.getLogger(DefaultPageViewsService.class);

    public DefaultPageViewsService(Path workspace) throws IOException {
        if (!Files.exists(workspace)) {
            Files.createDirectories(workspace);
        }
        this.pageViewsSource = new WikimediaPageViewsSource(workspace, NUM_PARALLEL_DOWNLOADS);
        this.topPageViewsCache = new TopViewsFSCache(workspace);

        PageViewsBlacklist blacklist = PageViewsBlacklistReader.getBlackList(workspace);
        this.pageViewsProcessor = new PageViewsProcessor(NUM_PARALLEL_PROCESSES, blacklist);
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

    //Kind of a gross way to run this. Ideally this is fronted by a Http Restful server.
    public static void main(String[] args) {
        DateAndHour to = new DateAndHour(LocalDate.now().minusYears(1), 10);
        DateAndHour from = new DateAndHour(LocalDate.now().minusYears(1), 3);

        Path workspace = Paths.get("~/tmp/pageviews");
        try (PageViewsService pageViewsService = new DefaultPageViewsService(workspace)) {
            Map<DateAndHour, TopPageViews> topViews = pageViewsService.getTopPageViews(from, to);
            System.out.println(topViews);
        } catch (Exception e) {
            //do nothing
        }
    }
}
