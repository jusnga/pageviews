package org.jusnga.pageviews;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class PageViews implements Comparable<PageViews> {
    private final String domainCode;
    private final String page;
    private final long views;

    @JsonCreator
    public PageViews(
            @JsonProperty("domainCode") String domainCode,
            @JsonProperty("page") String page,
            @JsonProperty("views") long views) {
        this.domainCode = domainCode;
        this.page = page;
        this.views = views;
    }

    @JsonProperty("domainCode")
    public String getDomainCode() {
        return domainCode;
    }

    @JsonProperty("page")
    public String getPage() {
        return page;
    }

    @JsonProperty("views")
    public long getViews() {
        return views;
    }

    @Override
    public String toString() {
        return "PageViews{" +
                "domainCode='" + domainCode + '\'' +
                ", page='" + page + '\'' +
                ", views=" + views +
                '}';
    }

    @Override
    public int compareTo(PageViews o) {
        return Long.compare(getViews(), o.getViews());
    }
}
