package org.cognitor.cassandra.migration.resolver;

import java.io.IOException;
import java.net.URL;
import java.util.Set;

/**
 * Scans for all resources in a given location.
 *
 * @author Patrick Kranz
 */
public interface ClassPathLocationScanner {
    /**
     * Finds the resource names below this location on the classpath under this locationUrl.
     *
     * @param location    The system-independent location on the classpath.
     * @param locationUrl The system-specific physical location URL.
     * @return The system-independent names of the resources on the classpath in a sorted set.
     * @throws IOException when the scanning failed.
     */
    Set<String> findResourceNames(String location, URL locationUrl) throws IOException;
}
