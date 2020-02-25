package org.jusnga.pageviews.processors;

import com.google.common.collect.Lists;
import org.jusnga.pageviews.PageViews;

import java.util.*;

public final class TopViewsProcessor implements Processor {
    private int numTopViews;
    private final Queue<PageViews> topPageViews;

    public TopViewsProcessor(int numTopViews) {
        this.numTopViews = numTopViews;
        this.topPageViews = new PriorityQueue<>(numTopViews);
    }

    @Override
    public void process(PageViews pageViews) {
        topPageViews.add(pageViews);
        if (topPageViews.size() > numTopViews) {
            topPageViews.poll();
        }
    }

    public List<PageViews> getTopPageViews() {
        List<PageViews> result =  Lists.newArrayList(topPageViews);
        Collections.sort(result);
        return result;
    }
}
