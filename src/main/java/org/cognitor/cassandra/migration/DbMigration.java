package org.cognitor.cassandra.migration;


import static org.cognitor.cassandra.migration.util.Ensure.notNull;
import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;

/**
 * An object representing a database migration. Every script corresponds to one object of this class.
 *
 * @author Patrick Kranz
 */
public class DbMigration {
    private final String migrationScript;
    private final String scriptName;
    private final int version;

    /**
     * Creates a new instance based on the given information.
     *
     * @param scriptName      the name of the script without the version part. Must not be null.
     * @param version         the schema version this migration will result to.
     * @param migrationScript the migration steps in cql. Must not be null.
     */
    public DbMigration(String scriptName, int version, String migrationScript) {
        this.migrationScript = notNull(migrationScript, "migrationScript");
        this.scriptName = notNullOrEmpty(scriptName, "scriptName");
        this.version = version;
    }

    public String getMigrationScript() {
        return migrationScript;
    }

    public String getScriptName() {
        return scriptName;
    }

    public int getVersion() {
        return version;
    }
}
