package org.cognitor.cassandra.migration.scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toSet;
import static org.cognitor.cassandra.migration.util.Ensure.notNull;
import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;

/**
 * Scans a path within a compiled jar for resources that are inside this path.
 *
 * @author Pavel Borsky
 */
public class JarLocationScanner implements LocationScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(JarLocationScanner.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> findResourceNames(String location, URI locationUri) throws IOException {
        notNullOrEmpty(location, "location");
        notNull(locationUri, "locationUri");
        LOGGER.debug("Scanning in jar {} in location {}", locationUri, location);
        try(FileSystem fileSystem = getFileSystem(locationUri)) {
            final Path systemPath = fileSystem.getPath(location);
            return Files.walk(systemPath)
                    .filter(Files::isRegularFile)
                    .map(path -> normalizePath(path.toString()))
                    .collect(toSet());
        }
    }

    private FileSystem getFileSystem(URI location) throws IOException {
        try {
            LOGGER.debug("Trying to get existing filesystem for {}", location.toString());
            return FileSystems.getFileSystem(location);
        } catch (FileSystemNotFoundException exception) {
            LOGGER.debug("Creating new filesystem for {}", location);
            return FileSystems.newFileSystem(location, emptyMap());
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
