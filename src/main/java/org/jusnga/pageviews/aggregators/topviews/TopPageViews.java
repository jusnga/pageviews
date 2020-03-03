package org.jusnga.pageviews.aggregators.topviews;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.jusnga.pageviews.PageViews;

import java.util.List;

public class TopPageViews {
    private final List<PageViews> topViews;

    public TopPageViews(@JsonProperty("topViews") List<PageViews> topViews) {
        this.topViews = topViews;
    }

    @JsonProperty("topViews")
    public List<PageViews> getTopViews() {
        return topViews;
    }

    @Override
    public String toString() {
        return "TopPageViews{" +
                "topViews=" + topViews +
                '}';
    }
}
