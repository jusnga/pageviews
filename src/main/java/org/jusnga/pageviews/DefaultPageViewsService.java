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
 * up capabilities for more streamlined processing. I'd ideally model this as a multi-stage pipeline to
 * decouple the downloading/processing. The way this would be modelled is that
 * each "stage" in the pipeline (i.e. download -> parse -> filter -> aggregate) would be an independent producer/consumer,
 * that can subscribe to any compatible producer. This allows for a number of nice functionalities, for e.g.
 * 1) You could start processing files as soon as they're done as opposed to waiting for all files to download
 * 2) You could have multiple consumers performing different thinks in parallel, e.g. in this particular solution the file
 * is first downloaded, then read/parsed. With a URL stream you could have a consumer that sinks to a file and another
 * that parses the input stream.
 *
 * Could probably implement this as a multi threaded java application but you're almost re-inventing kafka with spark/flink,
 * for a production tool you'd probably want to leverage kafka for the messaging/durability layer and something like
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

    public static void main(String[] args) {
        DateAndHour to = new DateAndHour(LocalDate.now().minusYears(1), 10);
        DateAndHour from = new DateAndHour(LocalDate.now().minusYears(1), 8);

        Path workspace = Paths.get("C:\\Users\\Justin\\Documents\\pageviews");
        try (PageViewsService pageViewsService = new DefaultPageViewsService(workspace)) {
            Map<DateAndHour, TopPageViews> topViews = pageViewsService.getTopPageViews(from, to);
            System.out.println(topViews);
        } catch (Exception e) {
            //do nothing
        }

        int i=0;
    }
}
