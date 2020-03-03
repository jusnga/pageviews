package org.jusnga.pageviews.sources.dumpswikimedia;

import org.jusnga.pageviews.PageViews;
import org.jusnga.pageviews.sources.PageViewsResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

public final class WikimediaPageViewsResource implements PageViewsResource {
    private BufferedReader reader;

    private final Path filePath;

    private static final Logger logger = LoggerFactory.getLogger(WikimediaPageViewsResource.class);

    public WikimediaPageViewsResource(Path filePath) {
        this.filePath = filePath;
    }

    @Override
    public Iterator<PageViews> iterator() throws IOException {
        InputStream fileStream = new FileInputStream(filePath.toFile());
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream);
        reader = new BufferedReader(decoder);
        return new PageViewsIterator(reader.lines().iterator());
    }

    @Override
    public void close() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    public String toString() {
        return "WikimediaPageViewsResource{" +
                "filePath=" + filePath +
                '}';
    }

    private static class PageViewsIterator implements Iterator<PageViews> {
        private PageViews next;

        private final Iterator<String> delegate;

        private PageViewsIterator(Iterator<String> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            }

            Optional<PageViews> nextPageViews = getNextPageViews();
            if (!nextPageViews.isPresent()) {
                return false;
            }

            next = nextPageViews.get();

            return true;
        }

        private Optional<PageViews> getNextPageViews() {
            while (delegate.hasNext()) {
                String nextLine = delegate.next();
                String[] values = nextLine.replaceAll("\\s+", " ").split(" ");
                if (values.length != 4) {
                    logger.warn("Invalid page view counts value {}", nextLine);
                    continue;
                }

                PageViews pageViews = new PageViews(values[0], values[1], Integer.parseInt(values[2]));

                return Optional.of(pageViews);
            }

            return Optional.empty();
        }

        @Override
        public PageViews next() {
            if (!hasNext()) {
                throw new IllegalStateException("No next value");
            }

            PageViews toReturn = next;
            next = null;

            return toReturn;
        }
    }
}
