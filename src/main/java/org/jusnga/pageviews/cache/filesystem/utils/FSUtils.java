package org.jusnga.pageviews.cache.filesystem.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

public final class FSUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static <T> void writeObject(T object, String fileName, Path parentDirectoryPath) throws IOException {
        Path tmpFilePath = Files.createTempFile(parentDirectoryPath, "", ".tmp");

        objectMapper.writeValue(tmpFilePath.toFile(), object);

        Path filePath = parentDirectoryPath.resolve(fileName);

        Files.move(tmpFilePath, filePath, StandardCopyOption.ATOMIC_MOVE);
    }

    public static <T> Optional<T> readObject(Path filePath, Class<T> clazz) throws IOException {
        File file = filePath.toFile();
        if (!file.exists()) {
            return Optional.empty();
        }

        return Optional.of(objectMapper.readValue(file, clazz));
    }
}
