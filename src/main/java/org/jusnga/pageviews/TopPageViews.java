package org.jusnga.pageviews;

import com.fasterxml.jackson.annotation.JsonProperty;

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

    public static TopPageViews from(List<PageViews> pageViews) {
        return new TopPageViews(pageViews);
    }
}
