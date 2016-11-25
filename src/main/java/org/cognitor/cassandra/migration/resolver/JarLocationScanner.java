package org.cognitor.cassandra.migration.resolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toSet;

/**
 * Scans a path within a compiled jar for resources that are inside this path.
 *
 * @author Pavel Borsky
 */
public class JarLocationScanner implements ClassPathLocationScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(JarLocationScanner.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> findResourceNames(String location, URI locationUri) throws IOException {
        LOGGER.info("Scanning in jar {} in location {}", locationUri, location);
        try(FileSystem fileSystem = FileSystems.newFileSystem(locationUri, emptyMap())) {
            final Path systemPath = fileSystem.getPath(location);
            return Files.walk(systemPath)
                    .filter(Files::isRegularFile)
                    .map(path -> normalizePath(path.toString()))
                    .collect(toSet());
        }
    }

    private static String normalizePath(String pathName) {
        if (pathName.startsWith("/")) {
            return pathName.substring(1);
        } else {
            return pathName;
        }
    }
}
