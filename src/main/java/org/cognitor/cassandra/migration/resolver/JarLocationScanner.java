package org.cognitor.cassandra.migration.resolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

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
        final FileSystem fileSystem = FileSystems.newFileSystem(locationUri, new HashMap<>());
        final Path systemPath = fileSystem.getPath(location);
        final TreeSet<String> resourceNames = new TreeSet<>();
        final Iterator<Path> iterator = Files.walk(systemPath).iterator();

        while (iterator.hasNext()) {
            final Path path = iterator.next();

            if (isRegularFile(path)) {
                final String pathName = path.toString();

                if (pathName.startsWith("/")) {
                    resourceNames.add(pathName.substring(1));
                } else {
                    resourceNames.add(pathName);
                }
                LOGGER.debug("Adding regular file: {}", path.getFileName());
            } else {
                LOGGER.debug("Skipping path: {}", path);
            }
        }

        return resourceNames;
    }

    /**
     * Reads isRegularFile attribute and returns it value
     *
     * @param path Path to file/directory/symbolic link
     * @return is path point to regular file or not
     */
    private static boolean isRegularFile(Path path) {
        try {
            final Object isRegularFile = Files.readAttributes(path, "isRegularFile").get("isRegularFile");
            if (isRegularFile instanceof Boolean) {
                return (Boolean) isRegularFile;
            } else {
                LOGGER.warn("Reading file attribute isRegularFile for path {} returned unexpected result: {}", path, isRegularFile);
                return false;
            }
        } catch (IOException e) {
            LOGGER.error("Failed on path reading: {}", path);
            return false;
        }
    }
}
