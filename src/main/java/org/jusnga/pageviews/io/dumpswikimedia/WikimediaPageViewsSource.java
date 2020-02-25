package org.jusnga.pageviews.io.dumpswikimedia;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.jusnga.pageviews.DateAndHour;
import org.jusnga.pageviews.PageViews;
import org.jusnga.pageviews.io.PageViewsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;

public class WikimediaPageViewsSource implements PageViewsSource {
    private static final Logger logger = LoggerFactory.getLogger(WikimediaPageViewsSource.class);

    @Override
    public List<PageViews> getPageViews(DateAndHour dateAndHour) {
        PageViewsLocator locator = PageViewsLocator.getLocator(dateAndHour);

        try {
            File tmpFile = Files.createTempFile("pageviews", ".count").toFile();
            tmpFile.deleteOnExit();

            URL fileUrl = locator.getUrl();

            logger.info("Downloading file {}", fileUrl);

            FileUtils.copyURLToFile(fileUrl, tmpFile);

            logger.info("Downloaded file {} of size {} bytes", fileUrl, FileUtils.sizeOf(tmpFile));

            return PageViewsParser.parsePageViews(tmpFile);
        } catch (IOException e) {
            logger.warn("Unable to download file for {}", locator);
        }

        return Lists.newArrayList();
    }
}
