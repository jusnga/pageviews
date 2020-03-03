package org.jusnga.pageviews.aggregators.topviews;

import com.google.common.collect.Lists;
import org.jusnga.pageviews.AtomicTaskRunner;
import org.jusnga.pageviews.DateAndHour;
import org.jusnga.pageviews.PageViews;
import org.jusnga.pageviews.aggregators.topviews.blacklist.PageViewsBlacklist;
import org.jusnga.pageviews.sources.PageViewsResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class PageViewsProcessor implements AutoCloseable {
    private final AtomicTaskRunner<DateAndHour, TopPageViews> taskRunner;
    private final PageViewsBlacklist blacklist;

    private static final int NUM_TOP_RESULTS = 25;

    public PageViewsProcessor(int numParallelProcesses, PageViewsBlacklist blacklist) {
        this.taskRunner = new AtomicTaskRunner<>(numParallelProcesses);
        this.blacklist = blacklist;
    }

    public Map<DateAndHour, TopPageViews> getTopPageViews(Map<DateAndHour, PageViewsResource> resourcesToProcess) {
        Map<DateAndHour, Callable<TopPageViews>> tasks = resourcesToProcess.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new TopViewsTask(entry.getValue(), blacklist, NUM_TOP_RESULTS)
                ));

        return taskRunner.runTasks(tasks);
    }

    @Override
    public void close() throws Exception {
        taskRunner.close();
    }

    public static class TopViewsTask implements Callable<TopPageViews> {
        private final PageViewsResource resource;
        private final PageViewsBlacklist blacklist;
        private final int numTopViews;
        private final PriorityQueue<PageViews> topViews;

        private static Logger logger = LoggerFactory.getLogger(TopViewsTask.class);

        public TopViewsTask(PageViewsResource resource, PageViewsBlacklist blacklist, int numTopViews) {
            this.resource = resource;
            this.blacklist = blacklist;
            this.numTopViews = numTopViews;
            this.topViews = new PriorityQueue<>(numTopViews + 1);
        }

        @Override
        public TopPageViews call() throws Exception {
            try {
                logger.info("Processing top views for resource {}", resource);
                Iterator<PageViews> iterator = resource.iterator();
                iterator.forEachRemaining(pageViews -> {
                    if (blacklist.isBlacklisted(pageViews)) {
                        logger.debug("Encountered blacklisted entry {}", pageViews);
                        return;
                    }

                    topViews.add(pageViews);
                    if (topViews.size() > numTopViews) {
                        topViews.poll();
                    }
                });
            } finally {
                resource.close();
            }

            List<PageViews> result = Lists.newArrayList(topViews);
            Collections.sort(result);
            return new TopPageViews(result);
        }
    }
}
