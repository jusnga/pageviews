package org.jusnga.pageviews.sources.dumpswikimedia;

import com.google.common.collect.Maps;
import org.joda.time.LocalDateTime;
import org.jusnga.pageviews.DateAndHour;
import org.jusnga.pageviews.sources.PageViewsResource;
import org.jusnga.pageviews.sources.PageViewsSource;
import org.jusnga.pageviews.AtomicTaskRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class WikimediaPageViewsSource implements PageViewsSource {
    private final Path downloadPath;
    private final AtomicTaskRunner<DateAndHour, Path> downloadManager;

    private static final String DOWNLOAD_DIR_NAME = "downloads";
    private static final String PAGE_VIEW_DATE_FMT = "yyyyMMdd-HHmmss";
    private static final String PAGE_VIEW_FILE_FMT = "pageviews-%s.gz";
    private static final String YEAR_MONTH_PATH_FMT = "yyyy-MM";
    private static final String BASE_URL = "https://dumps.wikimedia.org/other/pageviews/";
    private static final Logger logger = LoggerFactory.getLogger(WikimediaPageViewsSource.class);

    public WikimediaPageViewsSource(Path workspace, int numParallelDownloads) throws IOException {
        Path downloadPath = workspace.resolve(DOWNLOAD_DIR_NAME);
        if (!Files.exists(downloadPath)) {
            Files.createDirectories(downloadPath);
        }

        this.downloadPath = downloadPath;
        this.downloadManager = new AtomicTaskRunner<>(numParallelDownloads);
    }

    @Override
    public Map<DateAndHour, PageViewsResource> getPageViewsResource(List<DateAndHour> dateAndHours) {
        Map<DateAndHour, PageViewsResource> resources = Maps.newHashMap();

        Iterator<DateAndHour> it = dateAndHours.iterator();
        while (it.hasNext()) {
            DateAndHour next = it.next();

            Path localFilePath = getLocalFilePath(next);
            if (!Files.exists(localFilePath)) {
                continue;
            }

            logger.info("Found previously downloaded file for {} at {}", next, localFilePath);
            resources.put(next, new WikimediaPageViewsResource(localFilePath));
            it.remove();
        }

        Map<DateAndHour, Callable<Path>> downloadTasks = dateAndHours.stream()
                .collect(Collectors.toMap(
                        dateAndHour -> dateAndHour,
                        this::getDownloadTask
                ));

        long start = System.currentTimeMillis();
        Map<DateAndHour, Path> responses = downloadManager.runTasks(downloadTasks);
        logger.info("Downloaded {} requests in {} ms", responses.size(), System.currentTimeMillis() - start);

        responses.forEach((dateAndHour, downloadedFile) -> {
            WikimediaPageViewsResource maybeCachedResource = tryCacheAndGetResource(dateAndHour, downloadedFile);

            resources.put(dateAndHour, maybeCachedResource);
        });

        return resources;
    }

    private Callable<Path> getDownloadTask(DateAndHour dateAndHour) {
        try {
            URL fileUrl = getFileUrl(dateAndHour);

            return new WikimediaDownloadTask(fileUrl);
        } catch (MalformedURLException e) {
            logger.error("Unable to form URL for {}", dateAndHour, e);
            throw new IllegalStateException(e);
        }
    }

    private WikimediaPageViewsResource tryCacheAndGetResource(DateAndHour dateAndHour, Path downloadedFile) {
        try {
            Path filePath = getLocalFilePath(dateAndHour);
            Path parentPath = filePath.getParent();
            if (!Files.exists(parentPath)) {
                Files.createDirectories(parentPath);
            }

            Files.move(downloadedFile, filePath, StandardCopyOption.ATOMIC_MOVE);

            return new WikimediaPageViewsResource(filePath);
        } catch (IOException e) {
            logger.warn("Unable to cache download for {}", dateAndHour, e);
            return new WikimediaPageViewsResource(downloadedFile);
        }
    }

    private URL getFileUrl(DateAndHour dateAndHour) throws MalformedURLException {
        LocalDateTime dateTime = dateAndHour.getLocalDateTime();
        URL basePath = new URL(BASE_URL);
        URL yearPath = new URL(basePath, dateTime.getYear() + "/");
        URL monthPath = new URL(yearPath,dateTime.toString(YEAR_MONTH_PATH_FMT) + "/");
        return new URL(monthPath, getFileName(dateAndHour));
    }

    private Path getLocalFilePath(DateAndHour dateAndHour) {
        return downloadPath.resolve(Integer.toString(dateAndHour.getYear()))
                .resolve(Integer.toString(dateAndHour.getMonth()))
                .resolve(getFileName(dateAndHour));
    }

    public String getFileName(DateAndHour dateAndHour) {
        LocalDateTime dateTime = dateAndHour.getLocalDateTime();
        return String.format(PAGE_VIEW_FILE_FMT, dateTime.toString(PAGE_VIEW_DATE_FMT));
    }

    @Override
    public void close() throws Exception {
        downloadManager.close();
    }
}
