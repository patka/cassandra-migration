package org.cognitor.cassandra.migration.spring.scanner;

import org.cognitor.cassandra.migration.scanner.JarLocationScanner;
import org.cognitor.cassandra.migration.scanner.LocationScanner;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * A location scanner that works inside a Spring Boot application
 * that is run from a fat jar.
 * If you want to use this manually without executing the
 * auto configuration for this module, you have to replace the
 * standard {@link JarLocationScanner} with this one by executing
 * <p>
 * <code>
 *      ScannerRegistry registry = new ScannerRegistry();
 *      registry.register(ScannerRegistry.JAR_SCHEME, new SpringBootLocationScanner());
 * </code>
 * </p>
 * Then set this registry on the {@link org.cognitor.cassandra.migration.MigrationRepository}
 *
 * This code was provided by
 * <a href="https://github.com/backjo">backjo</a>. Thanks a lot :)
 */
public class SpringBootLocationScanner implements LocationScanner {
    private PathMatchingResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver();

        @Override
        public Set<String> findResourceNames(String location, URI locationUri) throws IOException {
            Resource[] resources = resourcePatternResolver.getResources(location + "*.cql");

            Set<String> resourcePaths = new HashSet<>();
            for (Resource resource : resources) {
                resourcePaths.add(location + resource.getFilename());
            }
            return resourcePaths;
        }}
