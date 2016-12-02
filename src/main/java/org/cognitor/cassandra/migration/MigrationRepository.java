package org.cognitor.cassandra.migration;

import org.cognitor.cassandra.migration.resolver.ClassPathLocationScanner;
import org.cognitor.cassandra.migration.resolver.FileSystemLocationScanner;
import org.cognitor.cassandra.migration.collector.FailOnDuplicatesCollector;
import org.cognitor.cassandra.migration.collector.ScriptCollector;
import org.cognitor.cassandra.migration.collector.ScriptFile;

import org.cognitor.cassandra.migration.resolver.JarLocationScanner;
import org.cognitor.cassandra.migration.resolver.ScannerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.regex.Pattern;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Collections.sort;
import static org.cognitor.cassandra.migration.util.Ensure.notNull;
import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;

/**
 * <p>
 * This class represents the collection of scripts that contain database migrations. It will scan the given location for
 * scripts that can be executed and analyzes the version of the scripts.
 * </p>
 * <p>
 * Only scripts that end with <code>SCRIPT_EXTENSION</code> will be considered.
 * </p>
 * <p>
 * Within a script every line starting with <code>COMMENT_PREFIX</code> will be ignored.
 * </p>
 *
 * @author Patrick Kranz
 */
public class MigrationRepository {
    /**
     * The default location in the classpath to check for migration scripts
     */
    public static final String DEFAULT_SCRIPT_PATH = "cassandra/migration";
    /**
     * The script extension for migrations. Every file that not ends with this extension will not be considered.
     */
    public static final String SCRIPT_EXTENSION = ".cql";

    /**
     * The encoding that es expected from the cql files.
     */
    public static final String SCRIPT_ENCODING = "UTF-8";

    /**
     * The delimiter that needs to be placed between the version and the name of the script.
     */
    public static final String VERSION_NAME_DELIMITER = "_";

    /**
     * Pattern matching the prefixes that can be put in the beginning of a line to indicate a single line comment.
     * Any line matching this pattern will be ignored.
     */
    public static final String SINGLE_LINE_COMMENT_PATTERN = "(^--.*)|(^//.*)";

    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationRepository.class);
    private static final String EXTRACT_VERSION_ERROR_MSG = "Error for script %s. Unable to extract version.";
    private static final String SCANNING_SCRIPT_FOLDER_ERROR_MSG = "Error while scanning script folder for new scripts.";
    private static final String READING_SCRIPT_ERROR_MSG = "Error while reading script %s";

    private final String scriptPath;
    private final Pattern commentPattern;
    private final ScannerFactory scannerFactory;
    private List<ScriptFile> migrationScripts;
    private ScriptCollector scriptCollector;

    /**
     * Creates a new repository with the <code>DEFAULT_SCRIPT_PATH</code> configured and a
     * {@link ScriptCollector} that will throw an exception in case
     * there are duplicate versions inside the repository.
     *
     * @throws MigrationException in case there is a problem reading the scripts in the path or
     *                            the repository contains duplicate script versions
     */
    public MigrationRepository() {
        this(DEFAULT_SCRIPT_PATH);
    }

    /**
     * Creates a new repository with the given scriptPath and the default
     * {@link ScriptCollector} that will throw an exception in case
     * there are duplicate versions inside the repository.
     *
     * @param scriptPath the path on the classpath to the migration scripts. Must not be null.
     * @throws MigrationException in case there is a problem reading the scripts in the path or
     *                            the repository contains duplicate script versions
     */
    public MigrationRepository(String scriptPath) {
        this(scriptPath, new FailOnDuplicatesCollector());
    }

    /**
     * Creates a new repository with the given scriptPath and the given
     * {@link ScriptCollector}.
     *
     * @param scriptPath the path on the classpath to the migration scripts. Must not be null.
     * @param scriptCollector the collection strategy used to collect the scripts. Must not be null.
     * @throws MigrationException in case there is a problem reading the scripts in the path.
     */
    public MigrationRepository(String scriptPath, ScriptCollector scriptCollector) {
        this.scriptCollector = notNull(scriptCollector, "scriptCollector");
        this.scriptPath = normalizePath(notNullOrEmpty(scriptPath, "scriptPath"));
        this.commentPattern = Pattern.compile(SINGLE_LINE_COMMENT_PATTERN);
        this.scannerFactory = new ScannerFactory();
        try {
            migrationScripts = scanForScripts(scriptPath);
        } catch (IOException | URISyntaxException exception) {
            throw new MigrationException(SCANNING_SCRIPT_FOLDER_ERROR_MSG, exception);
        }
    }

    /**
     * Ensures that every path starts and ends with a slash character.
     *
     * @param scriptPath the scriptPath that needs to be normalized
     * @return a path with leading and trailing slash
     */
    private String normalizePath(String scriptPath) {
        StringBuilder builder = new StringBuilder(scriptPath.length() + 1);
        if (scriptPath.startsWith("/")) {
            builder.append(scriptPath.substring(1));
        } else {
            builder.append(scriptPath);
        }
        if (!scriptPath.endsWith("/")) {
            builder.append("/");
        }
        return builder.toString();
    }

    /**
     * Gets the version of the scripts. This version represents the highest version that can be found in the scripts,
     * meaning the script with the highest version will be the one defining the version that is returned here.
     * In case the directory is empty zero will be returned as a version number.
     *
     * @return the latest version of the migrations, or zero if the directory contains no scripts.
     */
    public int getLatestVersion() {
        if (migrationScripts.isEmpty()) {
            return 0;
        }
        return migrationScripts.get(migrationScripts.size() - 1).getVersion();
    }

    private List<ScriptFile> scanForScripts(String scriptPath) throws IOException, URISyntaxException {
        LOGGER.debug("Scanning for cql migration scripts in " + scriptPath);
        Enumeration<URL> scriptResources = getClass().getClassLoader().getResources(scriptPath);
        while (scriptResources.hasMoreElements()) {
            URI script = scriptResources.nextElement().toURI();
            LOGGER.debug("Potential script folder: {}", script.toString());
            ClassPathLocationScanner scanner = scannerFactory.getScanner(script.getScheme());
            for (String resource : scanner.findResourceNames(scriptPath, script)) {
                if (isMigrationScript(resource)) {
                    String scriptName = extractScriptName(resource);
                    int version = extractScriptVersion(scriptName);
                    scriptCollector.collect(new ScriptFile(version, resource, scriptName));
                } else {
                    LOGGER.warn(format("Ignoring file %s because it is not a cql file.", resource));
                }
            }
        }
        List<ScriptFile> scripts = new ArrayList<>(scriptCollector.getScriptFiles());
        LOGGER.info(format("Found %d migration scripts", scripts.size()));
        sort(scripts);
        return scripts;
    }

    private static int extractScriptVersion(String scriptName) {
        String[] splittedName = scriptName.split(VERSION_NAME_DELIMITER);
        try {
            return parseInt(splittedName[0]);
        } catch (NumberFormatException exception) {
            throw new MigrationException(format(EXTRACT_VERSION_ERROR_MSG, scriptName),
                    exception, scriptName);
        }
    }

    private static boolean isMigrationScript(String resource) {
        return resource.endsWith(SCRIPT_EXTENSION);
    }

    private String extractScriptName(String resourceName) {
        return resourceName.substring(scriptPath.length());
    }

    /**
     * Returns all migrations starting from and excluding the given version. Usually you want to provide the version of
     * the database here to get all migrations that need to be executed. In case there is no script with a newer
     * version than the one given, an empty list is returned.
     *
     * @param version the version that is currently in the database
     * @return all versions since the given version or an empty list if no newer script is available. Never null.
     *         Does not include the given version.
     */
    public List<DbMigration> getMigrationsSinceVersion(int version) {
        List<DbMigration> dbMigrations = new ArrayList<>();
        migrationScripts.stream().filter(script -> script.getVersion() > version).forEach(script -> {
            String content = loadScriptContent(script);
            dbMigrations.add(new DbMigration(script.getScriptName(), script.getVersion(), content));
        });
        return dbMigrations;
    }

    private String loadScriptContent(ScriptFile script) {
        try {
            return readResourceFileAsString(script.getResourceName(), getClass().getClassLoader());
        } catch (IOException exception) {
            throw new MigrationException(format(READING_SCRIPT_ERROR_MSG, script.getResourceName()),
                    exception, script.getScriptName());
        }
    }

    private String readResourceFileAsString(String resourceName, ClassLoader classLoader) throws IOException {
        StringBuilder fileContent = new StringBuilder(256);
        new BufferedReader(
                new InputStreamReader(classLoader.getResourceAsStream(resourceName), SCRIPT_ENCODING))
                    .lines().filter(line -> !isLineComment(line)).forEach(fileContent::append);
        return fileContent.toString();
    }

    private boolean isLineComment(String line) {
        return commentPattern.matcher(line).matches();
    }
}
