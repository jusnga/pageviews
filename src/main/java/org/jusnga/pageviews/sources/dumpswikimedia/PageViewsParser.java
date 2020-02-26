package org.jusnga.pageviews.sources.dumpswikimedia;

import com.google.common.io.CountingInputStream;
import org.jusnga.pageviews.PageViews;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public final class PageViewsParser {
    private static final Logger logger = LoggerFactory.getLogger(PageViewsParser.class);

    private PageViewsParser() {}

    public static List<PageViews> parsePageViews(File pageViewsFile) throws IOException {
        try (
            InputStream fileStream = new FileInputStream(pageViewsFile);
            InputStream gzipStream = new GZIPInputStream(fileStream);
            CountingInputStream countingInputStream = new CountingInputStream(gzipStream);
            BufferedReader reader = new BufferedReader(new InputStreamReader(countingInputStream))
        ) {
            List<PageViews> pageViews = reader.lines()
                    .map(row -> {
                        String[] values = row.replaceAll("\\s+", " ").split(" ");
                        if (values.length != 4) {
                            logger.warn("Invalid page view counts value {}", row);
                        }

                        return values;
                    })
                    .filter(values -> values.length == 4)
                    .map(values -> {
                        return new PageViews(values[0], values[1], Integer.parseInt(values[2]));
                    })
                    .collect(Collectors.toList());

            logger.info("Processed {} bytes for {}", countingInputStream.getCount(), pageViewsFile);

            return pageViews;
        }
    }
}
