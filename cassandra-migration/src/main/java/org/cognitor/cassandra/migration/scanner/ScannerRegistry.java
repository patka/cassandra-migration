package org.cognitor.cassandra.migration.scanner;

import java.util.HashMap;
import java.util.Map;

import static org.cognitor.cassandra.migration.util.Ensure.notNull;
import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;

/**
 * The <code>ScannerRegistry</code> is used to provide a {@link LocationScanner}
 * depending on the scheme that is provided. This default implementation
 * knows two types:
 * <ul>
 *     <li>Jar scanning</li>
 *     <li>Filesystem scanning</li>
 * </ul>
 * In case the provided scheme is "jar", the {@link JarLocationScanner} is returned. For
 * the scheme "file" the {@link FileSystemLocationScanner} is returned. If you want to include
 * you own scanner implementation you can register it here with the appropriate
 * scheme. If you need to overwrite on existing one you can do this as well by providing
 * the scheme you want to overwrite. You can use the constants of this class for convenience.
 *
 * The scheme names are considered case-insensitive, therefore "JAR" and "jar" will return
 * the same entry.
 *
 * @author Patrick Kranz
 */
public class ScannerRegistry {
    /**
     * The scheme used to register scanners for jar files
     */
    public static final String JAR_SCHEME = "jar";
    /**
     * The scheme used to register scanners for file locations
     */
    public static final String FILE_SCHEME = "file";

    private final Map<String, LocationScanner> scanners;

    public ScannerRegistry() {
        this.scanners = new HashMap<>();
        this.scanners.put(JAR_SCHEME, new JarLocationScanner());
        this.scanners.put(FILE_SCHEME, new FileSystemLocationScanner());
    }

    /**
     * Returns the correct implementation of a {@link LocationScanner}
     * depending on the scheme or null of no scanner for this scheme is
     * registered.
     *
     * @param scheme the scheme that should be supported
     * @return {@link JarLocationScanner} if scheme is "jar", {@link FileSystemLocationScanner}
     *      if the scheme is "file" or null if an unknown scheme is given
     * @throws IllegalArgumentException in case scheme is null or empty
     */
    public LocationScanner getScanner(String scheme) {
        notNullOrEmpty(scheme, "scheme");
        return this.scanners.get(scheme.toLowerCase());
    }

    /**
     * Indicates of there is s {@link LocationScanner} registered for
     * the given scheme.
     *
     * @param scheme the scheme for which a {@link LocationScanner} is required
     * @return true if a scanner is registered, false otherwise
     * @throws IllegalArgumentException in case scheme is null or empty
     */
    public boolean supports(String scheme) {
        notNullOrEmpty(scheme, "scheme");
        return this.scanners.containsKey(scheme.toLowerCase());
    }

    /**
     * Registers a new {@link LocationScanner}. If there is already one registered
     * for the provided scheme, the registration will be updated.
     *
     * @param scheme the scheme for which the scanner can handle
     * @param scanner the scanner implementation
     * @throws IllegalArgumentException if scheme is null or empty or scanner is null
     */
    public void register(String scheme, LocationScanner scanner) {
        notNullOrEmpty(scheme, "scheme");
        notNull(scanner, "scanner");
        this.scanners.put(scheme.toLowerCase(), scanner);
    }

    /**
     * Remove the scanner for the given scheme from the registry.
     * If no entry is inside the registry nothing will happen.
     *
     * @param scheme the scheme for which the scanner should be removed
     * @throws IllegalArgumentException in case scheme is null or empty
     */
    public void unregister(String scheme) {
        notNullOrEmpty(scheme, "scheme");
        this.scanners.remove(scheme);
    }
}
