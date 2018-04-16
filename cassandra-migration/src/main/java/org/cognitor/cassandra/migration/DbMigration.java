package org.cognitor.cassandra.migration;


import java.util.Date;
import java.util.zip.CRC32;

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
    private long crc32Checksum;
    private Date executedAt;

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
        this.crc32Checksum = calculateCRC32();
    }

    /**
     * Creates a new instance based on the given information. This constructor should normally
     * only be used if an already persisted migration is loaded from the database. Otherwise
     * you should use the other one which calculates the checksum for the script.
     *
     * @param scriptName      the name of the script without the version part. Must not be null.
     * @param version         the schema version this migration will result to.
     * @param migrationScript the migration steps in cql. Must not be null.
     * @param checksum        the checksum of the migrationScript.
     * @param executedAt      the timestamp on which the migration was executed. Must not be null.
     */
    public DbMigration(String scriptName, int version, String migrationScript, long checksum, Date executedAt) {
        this.migrationScript = notNull(migrationScript, "migrationScript");
        this.scriptName = notNullOrEmpty(scriptName, "scriptName");
        this.version = version;
        this.crc32Checksum = checksum;
        this.executedAt = notNull(executedAt, "executedAt");
    }


    private long calculateCRC32() {
        final CRC32 crc32 = new CRC32();
        crc32.update(migrationScript.getBytes());
        return crc32.getValue();
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

    /**
     * Returns the CRC32 checksum of the migration script.
     *
     * @return the CRC32 checksum
     */
    public long getChecksum() {
        return this.crc32Checksum;
    }

    /**
     * Returns the date and time when the migration was applied to the database.
     *
     * @return the date and time of the migration execution. Never null.
     */
    public Date getExecutedAt() {
        return executedAt;
    }
}
