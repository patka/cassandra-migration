package org.cognitor.cassandra.migration.scanner;

import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/**
 * @author Patrick Kranz
 */
public class ScannerRegistryTest {

    @Test
    public void shouldReturnJarLocationScannerWhenJarSchemeGiven() {
        LocationScanner scanner = new ScannerRegistry().getScanner("jar");
        assertThat(scanner, is(not(nullValue())));
        assertThat(JarLocationScanner.class.isAssignableFrom(scanner.getClass()), is(true));
    }

    @Test
    public void shouldIgnoreCaseWhenJarSchemeGiven() {
        LocationScanner scanner = new ScannerRegistry().getScanner("jAr");
        assertThat(scanner, is(not(nullValue())));
        assertThat(JarLocationScanner.class.isAssignableFrom(scanner.getClass()), is(true));
    }

    @Test
    public void shouldReturnFileSystemLocationScannerWhenNonJarSchemeGiven() {
        LocationScanner scanner = new ScannerRegistry().getScanner("file");
        assertThat(scanner, is(not(nullValue())));
        assertThat(FileSystemLocationScanner.class.isAssignableFrom(scanner.getClass()), is(true));
    }

    @Test
    public void shouldReturnCustomScannerWhenScannerForNewSchemeGiven() {
        CustomScanner customScanner = new CustomScanner();
        ScannerRegistry registry = new ScannerRegistry();
        registry.register("test", customScanner);
        assertThat(registry.getScanner("test"), is(equalTo(customScanner)));
    }

    @Test
    public void shouldReturnCustomScannerWhenScannerForExistingSchemeGiven() {
        CustomScanner customScanner = new CustomScanner();
        ScannerRegistry registry = new ScannerRegistry();
        registry.register("jar", customScanner);
        assertThat(registry.getScanner("jar"), is(equalTo(customScanner)));
    }

    @Test
    public void shouldUnregisterScannerWhenRegisteredScannerGiven() {
        CustomScanner customScanner = new CustomScanner();
        ScannerRegistry registry = new ScannerRegistry();
        registry.register("test", customScanner);
        assertThat(registry.supports("test"), is(true));
        registry.unregister("test");
        assertThat(registry.supports("test"), is(false));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenNoSchemeGiven() {
        new ScannerRegistry().getScanner(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenEmptySchemeGiven() {
        new ScannerRegistry().getScanner("");
    }
}

class CustomScanner implements LocationScanner {

    @Override
    public Set<String> findResourceNames(String location, URI locationUri) throws IOException {
        return emptySet();
    }
}