package org.cognitor.cassandra.migration.resolver;

import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;

/**
 * @author Patrick Kranz
 */
public class ScannerFactory {
    private static final String JAR_SCHEME = "jar";

    public ClassPathLocationScanner getScanner(String scheme) {
        notNullOrEmpty(scheme, "scheme");
        if (JAR_SCHEME.equals(scheme.toLowerCase())) {
            return new JarLocationScanner();
        }
        return new FileSystemLocationScanner();
    }
}
