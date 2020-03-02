package org.jusnga.pageviews.sources;

import org.jusnga.pageviews.PageViews;

import java.io.IOException;
import java.util.Iterator;

/**
 * We don't want to be loading the whole resource in memory as that adds unnecessary memory pressure, this is an attempt
 * at abstracting away the semantics of what the source data is (could be JDBC or kafka), sources I've usually interacted
 * with usually support some form of lazy loading of data.
 */
public interface PageViewsResource extends AutoCloseable {
    Iterator<PageViews> iterator() throws IOException;
}
