package org.cognitor.cassandra.migration;

import com.datastax.driver.core.ConsistencyLevel;
import org.cognitor.cassandra.migration.keyspace.KeyspaceDefinition;

import static org.cognitor.cassandra.migration.util.Ensure.notNull;
import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;

/**
 * This class is supposed to contain different options to tune
 * the way migrations are executed. It comes with reasonable
 * default settings that should ensure smooth migrations.
 *
 * @author Patrick Kranz
 */
public class Configuration {
    public static final String DEFAULT_MIGRATION_LOCATION = "/cassandra/migration";
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.QUORUM;
    private final KeyspaceDefinition keyspaceDefinition;
    private boolean createKeyspace = false;
    private boolean checksumValidation = true;
    private String migrationLocation = DEFAULT_MIGRATION_LOCATION;
    private boolean validateOnly = false;
    private boolean recalculateChecksumOnly = false;
    private boolean recalculateChecksum = false;

    /**
     * Creates a new <code>Configuration</code> instance.
     * The provided <code>KeyspaceDefinition</code> will be used in
     * order to connect to the database and to create the keyspace
     * if <code>setCreateKeyspace(true)</code> was called.
     * If the keyspace already exists only the keyspace name will be used.
     *
     * @param keyspaceDefinition the definition of the keyspace to be used. Must not be null.
     * @throws IllegalArgumentException if keyspaceDefinition is null
     */
    public Configuration(KeyspaceDefinition keyspaceDefinition) {
        this.keyspaceDefinition = notNull(keyspaceDefinition, "keyspaceDefinition");
    }

    /**
     * Creates a new <code>Configuration</code> instance to connect
     * to the given keyspace. This is just a convenience constructor
     * that creates the <code>KeyspaceDefinition</code> instance and
     * calls the appropriate constructor.
     *
     * If you use this constructor and set <code>createKeyspace</code>
     * to true a keyspace with the simple strategy will be created.
     *
     * @param keyspaceName the name of the keyspace to connect to. Must not be null or empty.
     * @throws IllegalArgumentException in case keyspace is null or empty
     */
    public Configuration(String keyspaceName) {
        this.keyspaceDefinition = new KeyspaceDefinition(notNullOrEmpty(keyspaceName, "keyspaceName"));
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * Sets the consistency level that should be used when the statements
     * against the cluster are executed. Default is
     * <code>ConsistencyLevel.QUORUM</code> so that always a majority
     * of the nodes has the migration confirmed.
     *
     * @param consistencyLevel the consistency level for migration execution. Never null.
     * @return this configuration instance
     * @throws IllegalArgumentException if consistencyLevel is null
     */
    public Configuration setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = notNull(consistencyLevel, "consistencyLevel");
        return this;
    }

    public boolean isCreateKeyspace() {
        return this.createKeyspace;
    }

    /**
     * Set whether the keyspace should be created in case it does not exist.
     * If set to yes, the information provided in the <code>KeyspaceDefinition</code>
     * will be used to setup the keyspace.
     *
     * @param createKeyspace true if a missing keyspace should be created, false otherwise.
     * @return this configuration instance
     */
    public Configuration setCreateKeyspace(boolean createKeyspace) {
        this.createKeyspace = createKeyspace;
        return this;
    }

    public KeyspaceDefinition getKeyspaceDefinition() {
        return this.keyspaceDefinition;
    }

    public String getKeyspaceName() {
        return this.keyspaceDefinition.getKeyspaceName();
    }

    public boolean isChecksumValidation() {
        return checksumValidation;
    }

    /**
     * Set this to true in order to have the checksum of all existing
     * scripts recalculated. Use this if you want to make sure no script
     * inside the application has been changed after it was applied.
     * <b>Be aware, that this includes changes done to comments.</b>
     *
     * @param checksumValidation true if ckecksums should be validated
     * @return this configuration instance
     */
    public Configuration setChecksumValidation(boolean checksumValidation) {
        this.checksumValidation = checksumValidation;
        return this;
    }

    /**
     * Configures that only the checksum of the existing migrations are validated.
     * No migration to newer versions will be done and no keyspace migration will
     * happen if this is true. This should purely be used for verification.
     * Default is false.
     *
     * @param validateOnly true if only checksums should be done, false otherwise.
     */
    public Configuration setValidateOnly(boolean validateOnly) {
        this.validateOnly = validateOnly;
        return this;
    }

    public boolean isValidateOnly() {
        return validateOnly;
    }

    public String getMigrationLocation() {
        return migrationLocation;
    }

    public Configuration setMigrationLocation(String migrationLocation) {
        this.migrationLocation = notNullOrEmpty(migrationLocation, "migrationLocation");
        return this;
    }

    /**
     * Indicates wheter this is a checksum recalculation run or not. Default is false.
     * @return true if checksums should be updated, false otherwise.
     */
    public boolean isRecalculateChecksumOnly() {
        return recalculateChecksumOnly;
    }

    /**
     * Sets if the current execution should only recalculate the checksums. Default is false.
     * <b>If this is true, no other task will be executed.</b>
     *
     * @param recalculateChecksumOnly true if checksums should be updated. False otherwise.
     * @return this configuration instance
     */
    public Configuration setRecalculateChecksumOnly(boolean recalculateChecksumOnly) {
        this.recalculateChecksumOnly = recalculateChecksumOnly;
        return this;
    }

    public boolean isRecalculateChecksum() {
        return recalculateChecksum;
    }

    public Configuration setRecalculateChecksum(boolean recalculateChecksum) {
        this.recalculateChecksum = recalculateChecksum;
        return this;
    }
}
