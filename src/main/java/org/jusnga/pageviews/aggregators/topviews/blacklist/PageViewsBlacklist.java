package org.jusnga.pageviews.aggregators.topviews.blacklist;

import com.google.common.collect.Maps;
import org.jusnga.pageviews.PageViews;

import java.util.Map;
import java.util.Set;

public class PageViewsBlacklist {
    //Could use a bloom filter here if blacklist got too large
    private final Map<String, Set<String>> blacklist = Maps.newHashMap();

    public PageViewsBlacklist(Map<String, Set<String>> blacklist) {
        this.blacklist.putAll(blacklist);
    }

    public boolean isBlacklisted(PageViews pageViews) {
        String domainCode = pageViews.getDomainCode();
        String page = pageViews.getPage();
        return blacklist.containsKey(domainCode) && blacklist.get(domainCode).contains(page);
    }
}
