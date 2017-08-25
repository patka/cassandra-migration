package org.cognitor.cassandra.migration.scanner;

import org.junit.Test;

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

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenNoSchemeGiven() {
        new ScannerRegistry().getScanner(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenEmptySchemeGiven() {
        new ScannerRegistry().getScanner("");
    }
}