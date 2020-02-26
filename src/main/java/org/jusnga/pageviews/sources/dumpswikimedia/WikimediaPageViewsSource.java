package org.jusnga.pageviews.sources.dumpswikimedia;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.jusnga.pageviews.DateAndHour;
import org.jusnga.pageviews.PageViews;
import org.jusnga.pageviews.sources.PageViewsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class WikimediaPageViewsSource implements PageViewsSource {
    private final Path downloadLocation;

    private static final Logger logger = LoggerFactory.getLogger(WikimediaPageViewsSource.class);

    public WikimediaPageViewsSource(Path downloadLocation) throws IOException {
        if (!Files.exists(downloadLocation)) {
            Files.createDirectories(downloadLocation);
        }
        this.downloadLocation = downloadLocation;
    }

    @Override
    public List<PageViews> getPageViews(DateAndHour dateAndHour) {
        PageViewsLocator locator = PageViewsLocator.getLocator(dateAndHour);

        try {
            File localFile = getFilePath(locator).toFile();

            if (localFile.exists()) {
                logger.info("Processing local file {}", localFile);
            } else {
                logger.info("No local version of {} found, downloading from source", localFile);

                URL fileUrl = locator.getUrl();
                FileUtils.copyURLToFile(fileUrl, localFile);

                logger.info("Downloaded file {} of size {} bytes", fileUrl, FileUtils.sizeOf(localFile));
            }

            return PageViewsParser.parsePageViews(localFile);
        } catch (IOException e) {
            logger.warn("Unable to download file for {}: {}", locator, e);
        }

        return Lists.newArrayList();
    }

    private Path getFilePath(PageViewsLocator locator) {
        return downloadLocation.resolve(Integer.toString(locator.getYear()))
                .resolve(Integer.toString(locator.getMonth()))
                .resolve(locator.getFileName());
    }
}
