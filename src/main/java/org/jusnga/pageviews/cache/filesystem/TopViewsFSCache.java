package org.jusnga.pageviews.cache.filesystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jusnga.pageviews.DateAndHour;
import org.jusnga.pageviews.TopPageViews;
import org.jusnga.pageviews.cache.ResultCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

/**
 * Basic file system cache, ideally we cache to a fast access layer like ES/SQL or w/e suits this better. Didn't pay
 * too much attention on FS perf as that's a can of complexity I would usually opt to defer to a DB.
 */
public class TopViewsFSCache implements ResultCache<DateAndHour, TopPageViews> {
    private final Path parentDirectory;
    private final ObjectMapper objectMapper = new ObjectMapper();

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
            writeObject(result, fileName, cachePath);
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
            return readObject(cachedResultPath, TopPageViews.class);
        } catch (IOException e) {
            logger.warn("Unable to read cached result {}: {}", key, e);
        }

        return Optional.empty();
    }

    private String getTopViewsFileName(DateAndHour key) {
        String fileName = key.getLocalDateTime().toString(TOP_VIEWS_DATE_TIME_FMT);

        return String.format(TOP_VIEWS_FILE_FMT, fileName);
    }

    private Path getCachePath(DateAndHour dateAndHour) {
        int year = dateAndHour.getYear();
        int month = dateAndHour.getMonth();

        return parentDirectory
                .resolve(Integer.toString(year))
                .resolve(Integer.toString(month));
    }

    public <T> void writeObject(T object, String fileName, Path parentDirectoryPath) throws IOException {
        Path tmpFilePath = Files.createTempFile(parentDirectoryPath, "", ".tmp");

        objectMapper.writeValue(tmpFilePath.toFile(), object);

        Path filePath = parentDirectoryPath.resolve(fileName);

        Files.move(tmpFilePath, filePath, StandardCopyOption.ATOMIC_MOVE);
    }

    public <T> Optional<T> readObject(Path filePath, Class<T> clazz) throws IOException {
        File file = filePath.toFile();
        if (!file.exists()) {
            return Optional.empty();
        }

        return Optional.of(objectMapper.readValue(file, clazz));
    }

    @Override
    public void close() throws Exception {
        //nothing to do
    }
}
