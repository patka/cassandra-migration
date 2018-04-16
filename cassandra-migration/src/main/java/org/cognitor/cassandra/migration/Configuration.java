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
    private boolean validateExistingScripts = true;
    private String migrationLocation = DEFAULT_MIGRATION_LOCATION;

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

    public boolean isValidateExistingScripts() {
        return validateExistingScripts;
    }

    public Configuration setValidateExistingScripts(boolean validateExistingScripts) {
        this.validateExistingScripts = validateExistingScripts;
        return this;
    }

    public String getMigrationLocation() {
        return migrationLocation;
    }

    public Configuration setMigrationLocation(String migrationLocation) {
        this.migrationLocation = notNullOrEmpty(migrationLocation, "migrationLocation");
        return this;
    }
}
