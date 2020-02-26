package org.jusnga.pageviews.cache.filesystem;

import org.jusnga.pageviews.DateAndHour;
import org.jusnga.pageviews.TopPageViews;
import org.jusnga.pageviews.cache.ResultCache;
import org.jusnga.pageviews.cache.filesystem.utils.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class TopViewsFSCache implements ResultCache<DateAndHour, TopPageViews> {
    private final Path parentDirectory;

    private static final String TOP_VIEWS_FILE_FMT = "%s.topviews";
    private static final String TOP_VIEWS_DATE_TIME_FMT = "yyyyMMdd-HH";

    private static final Logger logger = LoggerFactory.getLogger(TopViewsFSCache.class);


    public TopViewsFSCache(Path parentDirectory) throws IOException {
        if (!Files.exists(parentDirectory)) {
            Files.createDirectory(parentDirectory);
        }
        this.parentDirectory = parentDirectory;
    }

    @Override
    public void cacheResult(DateAndHour key, TopPageViews result) {
        Path cachePath = getCachePath(key);
        try {
            if (!Files.exists(cachePath)) {
                Files.createDirectories(cachePath);
            }

            String fileName = getTopViewsFileName(key);
            FSUtils.writeObject(result, fileName, cachePath);
        } catch (IOException e) {
            logger.warn("Unable to cache result {}: {}", key, e);
        }
    }

    @Override
    public Optional<TopPageViews> getCachedResult(DateAndHour key) {
        Path cachePath = getCachePath(key);
        String fileName = getTopViewsFileName(key);

        Path cachedResultPath = cachePath.resolve(fileName);

        try {
            return FSUtils.readObject(cachedResultPath, TopPageViews.class);
        } catch (IOException e) {
            logger.warn("Unable to read cached result {}: {}", key, e);
        }

        return Optional.empty();
    }

    private String getTopViewsFileName(DateAndHour key) {
        String fileName = DateAndHour.toLocalDateTime(key).toString(TOP_VIEWS_DATE_TIME_FMT);

        return String.format(TOP_VIEWS_FILE_FMT, fileName);
    }

    private Path getCachePath(DateAndHour dateAndHour) {
        int year = dateAndHour.getYear();
        int month = dateAndHour.getMonth();

        return parentDirectory
                .resolve(Integer.toString(year))
                .resolve(Integer.toString(month));
    }
}
