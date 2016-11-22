package org.cognitor.cassandra.migration.resolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static java.nio.file.FileSystems.newFileSystem;
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
     * Scans a path on the filesystem for resources inside the given classpath location.
     *
     * @param location    The system-independent location on the classpath.
     * @param locationUri The system-specific physical location URI.
     * @return a sorted set containing all the resources inside the given location
     * @throws IOException if an error accessing the filesystem happens
     */
    @Override
    public Set<String> findResourceNames(String location, URI locationUri) throws IOException {
        LOGGER.debug("Scanning in jar {} in location {}", location, locationUri);
        final FileSystem fileSystem = newFileSystem(locationUri, emptyMap());
        final Path systemPath = fileSystem.getPath(location);
        return Files.walk(systemPath)
                .filter(Files::isRegularFile)
                .map(path -> normalizePath(path.toString()))
                .collect(toSet());
    }

    private static String normalizePath(String pathName) {
        if (pathName.startsWith("/")) {
            return pathName.substring(1);
        } else {
            return pathName;
        }
    }
}
