package org.cognitor.cassandra.migration.resolver;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.util.Date;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author Patrick Kranz
 */
public class JarLocationScannerTest {

    private static File jarFile;
    private static URI jarUri;

    @BeforeClass
    public static void beforeClass() throws IOException, URISyntaxException {
        jarFile = createJar();
        jarUri = new URI(format("jar:file:%s", jarFile.toString().replace("\\", "/")));
    }

    @AfterClass
    public static void afterClass() throws IOException {
        if (jarFile != null) {
            jarFile.delete();
        }
    }

    @Test
    public void shouldReturnTwoResourcesWhenJarFileWithOneScriptGiven() throws Exception {
        ClassPathLocationScanner scanner = new JarLocationScanner();
        Set<String> resourceNames = scanner.findResourceNames("/", jarUri);
        assertThat(resourceNames.size(), is(equalTo(2)));
        assertThat(resourceNames.contains("1_init.cql"), is(true));
        assertThat(resourceNames.contains("META-INF/MANIFEST.MF"), is(true));
    }

    @Test(expected = NoSuchFileException.class)
    public void shouldThrowExceptionWhenNonExistingPathGiven() throws IOException {
        ClassPathLocationScanner scanner = new JarLocationScanner();
        Set<String> resourceNames = scanner.findResourceNames("/nonThere", jarUri);
        assertThat(resourceNames.size(), is(equalTo(0)));
    }

    private static File createJar() throws IOException {
        File jarFile = File.createTempFile("Test", ".jar");
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        try (JarOutputStream jarOutputStream = new JarOutputStream(new FileOutputStream(jarFile), manifest)) {
            InputStream inputStream = JarLocationScannerTest.class.getResourceAsStream("/cassandra/migrationtest/jarfile/1_init.cql");
            add("1_init.cql", inputStream,  jarOutputStream);
        }
        return jarFile;
    }

    private static void add(String filename, InputStream inputStream, JarOutputStream target) throws IOException {
        JarEntry entry = new JarEntry(filename);
        entry.setTime(new Date().getTime());
        target.putNextEntry(entry);
        try(BufferedInputStream bis = new BufferedInputStream(inputStream)) {
            byte[] buffer = new byte[1024];
            int count;
            for(;;) {
                count = bis.read(buffer);
                if (count == -1) {
                    break;
                }
                target.write(buffer, 0, count);
            }
        }
        target.closeEntry();
    }
}
