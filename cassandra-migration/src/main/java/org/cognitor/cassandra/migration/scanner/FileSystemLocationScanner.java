package org.cognitor.cassandra.migration.scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Set;
import java.util.TreeSet;

import static java.net.URLDecoder.decode;
import static java.util.Collections.emptySet;

/**
 * Scans a path on the filesystem for resources that are inside this path.
 * This class was heavily inspired by a similar class in the flyway project.
 *
 * @author Patrick Kranz
 */
public class FileSystemLocationScanner implements ClassPathLocationScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemLocationScanner.class);
    private static final String UTF_8 = "UTF-8";

    /**
     * Scans a path on the filesystem for resources inside the given classpath location.
     *
     * @param location    The system-independent location on the classpath.
     * @param locationUri The system-specific physical location URI.
     * @return a sorted set containing all the resources inside the given location
     * @throws IOException if an error accessing the filesystem happens
     */
    public Set<String> findResourceNames(String location, URI locationUri) throws IOException {
        String filePath = toFilePath(locationUri);
        File folder = new File(filePath);
        if (!folder.isDirectory()) {
            LOGGER.debug("Skipping path as it is not a directory: " + filePath);
            return new TreeSet<>();
        }

        String classPathRootOnDisk = filePath.substring(0, filePath.length() - location.length());
        if (!classPathRootOnDisk.endsWith(File.separator)) {
            classPathRootOnDisk = classPathRootOnDisk + File.separator;
        }
        LOGGER.debug("Scanning starting at classpath root in filesystem: " + classPathRootOnDisk);
        return findResourceNamesFromFileSystem(classPathRootOnDisk, location, folder);
    }

    /**
     * Finds all the resource names contained in this file system folder.
     *
     * @param classPathRootOnDisk The location of the classpath root on disk, with a trailing slash.
     * @param scanRootLocation    The root location of the scan on the classpath, without leading or trailing slashes.
     * @param folder              The folder to look for resources under on disk.
     * @return The resource names;
     */
    private Set<String> findResourceNamesFromFileSystem(String classPathRootOnDisk, String scanRootLocation, File folder) {
        LOGGER.debug("Scanning for resources in path: {} ({})", folder.getPath(), scanRootLocation);

        File[] files = folder.listFiles();
        if (files == null) {
            return emptySet();
        }

        Set<String> resourceNames = new TreeSet<>();

        for (File file : files) {
            if (file.canRead()) {
                if (file.isDirectory()) {
                    resourceNames.addAll(findResourceNamesFromFileSystem(classPathRootOnDisk, scanRootLocation, file));
                } else {
                    resourceNames.add(toResourceNameOnClasspath(classPathRootOnDisk, file));
                }
            }
        }

        return resourceNames;
    }

    /**
     * Converts this file into a resource name on the classpath by cutting of the file path
     * to the classpath root.
     *
     * @param classPathRootOnDisk The location of the classpath root on disk, with a trailing slash.
     * @param file                The file.
     * @return The resource name on the classpath.
     */
    private String toResourceNameOnClasspath(String classPathRootOnDisk, File file) {
        String fileName = file.getAbsolutePath().replace("\\", "/");
        return fileName.substring(classPathRootOnDisk.length());
    }

    private static String toFilePath(URI uri) {
        try {
            String filePath = new File(decode(uri.getPath().replace("+", "%2b"), UTF_8)).getAbsolutePath();
            if (filePath.endsWith("/")) {
                return filePath.substring(0, filePath.length() - 1);
            }
            return filePath;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Something really strange happened", e);
        }
    }
}
