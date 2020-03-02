package org.jusnga.pageviews.sources.dumpswikimedia;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

public class WikimediaDownloadTask implements Callable<Path> {
    private final URL url;

    private static final Logger logger = LoggerFactory.getLogger(WikimediaDownloadTask.class);

    public WikimediaDownloadTask(URL url) {
        this.url = url;
    }

    @Override
    public Path call() throws Exception {
        //Probably want to configure where we download tmp files to
        Path tmpPath = Files.createTempFile("", ".tmp");

        try (InputStream inputStream = url.openStream()) {
            logger.info("Starting download for {}", url);
            FileUtils.copyInputStreamToFile(inputStream, tmpPath.toFile());
        }

        logger.info("Successfully downloaded {} of size {} to {}", url, FileUtils.sizeOf(tmpPath.toFile()),
                tmpPath);

        return tmpPath;
    }
}
