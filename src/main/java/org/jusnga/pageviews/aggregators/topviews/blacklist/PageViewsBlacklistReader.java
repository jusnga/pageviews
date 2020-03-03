package org.jusnga.pageviews.aggregators.topviews.blacklist;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class PageViewsBlacklistReader {
    private static final String blacklistFileName = "blacklist_domains_and_pages";
    private static final String blacklistUrl = " https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages";
    private static final Logger logger = LoggerFactory.getLogger(PageViewsBlacklistReader.class);

    public static PageViewsBlacklist getBlackList(Path workspace) throws IOException {
        Path blacklistFile = getOrDownloadBlacklistFile(workspace);

        Map<String, Set<String>> blacklist = parseBlacklistFile(blacklistFile);

        return new PageViewsBlacklist(blacklist);
    }

    private static Map<String, Set<String>> parseBlacklistFile(Path blacklistPath) throws IOException {
        Map<String, Set<String>> blacklists = Maps.newHashMap();
        try (
                InputStream fileStream = new FileInputStream(blacklistPath.toFile());
                Reader decoder = new InputStreamReader(fileStream);
                BufferedReader reader = new BufferedReader(decoder)
        ) {
            Iterator<String> fileIterator = reader.lines().iterator();
            while(fileIterator.hasNext()) {
                String nextLine = fileIterator.next();

                String[] values = nextLine.replaceAll("\\s+", " ").split(" ");

                if (values.length != 2) {
                    logger.warn("Invalid blacklist entry: {}", nextLine);
                    continue;
                }

                String domain = values[0];
                String page = values[1];

                Set<String> pagesForDomain = blacklists.getOrDefault(domain, new HashSet<>());
                pagesForDomain.add(page);
                blacklists.put(domain, pagesForDomain);
            }
        }

        return blacklists;
    }

    private static Path getOrDownloadBlacklistFile(Path workspace) throws IOException {
        Path blacklistFile = workspace.resolve(blacklistFileName);
        if (Files.exists(blacklistFile)) {
            return blacklistFile;
        }

        URL url = new URL(blacklistUrl);
        try (InputStream urlStream = url.openStream()) {
            Path tmpPath = Files.createTempFile("", ".tmp");
            FileUtils.copyInputStreamToFile(urlStream, tmpPath.toFile());

            Files.move(tmpPath, blacklistFile, StandardCopyOption.ATOMIC_MOVE);
        }

        return blacklistFile;
    }
}
