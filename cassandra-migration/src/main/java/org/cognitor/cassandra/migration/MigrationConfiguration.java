package org.cognitor.cassandra.migration;

import org.cognitor.cassandra.migration.keyspace.Keyspace;

import static org.cognitor.cassandra.migration.util.Ensure.notNull;
import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;

/**
 * MigrationConfiguration contains the required settings for the {@link Database} to be
 * created and for later execution. It is the central place to modify the behavior of the
 * migration execution.
 *
 * Currently, the only required parameter is the keyspace name. Alternatively the instance
 * of the keyspace can be set as well as this contains the name. Only use one or the other
 * as internally it will always result in an instance of {@link Keyspace} meaning, if you
 * set an instance of {@link Keyspace} with a non default keyspace configuration and then
 * call <code>setKeyspaceName</code> separately, your non default keyspace configuration
 * will be lost.
 */
public class MigrationConfiguration {
    public final String EMPTY_TABLE_PREFIX = "";
    private String tablePrefix = EMPTY_TABLE_PREFIX;
    private Keyspace keyspace;
    private String executionProfile;

    /**
     * Set the name of the keyspace to be used. This is just a shortcut for
     * <code>setKeyspace(new Keyspace(keyspaceName))</code>.
     *
     * @param keyspaceName the name of the keyspace. Must not be null or empty.
     * @return this instance of the <code>MigrationConfiguration</code>. Never null.
     */
    public MigrationConfiguration withKeyspaceName(String keyspaceName) {
        this.keyspace = new Keyspace(notNullOrEmpty(keyspaceName, "keyspaceName"));
        return this;
    }

    /**
     * Sets the prefix that will be used whenever a table is created to manage migration scripts.
     * The default is no prefix which is done by using <code>EMPTY_TABLE_PREFIX</code>.
     *
     * @param tablePrefix the prefix to be used for any management table to be created. Can be null
     *                    in which case the <code>EMPTY_TABLE_PREFIX</code> will be used.
     * @return this instance of the <code>MigrationConfiguration</code>. Never null.
     */
    public MigrationConfiguration withTablePrefix(String tablePrefix) {
        if (tablePrefix == null) {
            this.tablePrefix = EMPTY_TABLE_PREFIX;
            return this;
        }
        this.tablePrefix = tablePrefix;
        return this;
    }

    /**
     * Sets the keyspace instance to be used for schema migration.
     *
     * @param keyspace the keyspace to be used for schema migration. Must not be null.
     * @return this instance of the <code>MigrationConfiguration</code>. Never null.
     */
    public MigrationConfiguration withKeyspace(Keyspace keyspace) {
        this.keyspace = notNull(keyspace, "keyspace");
        return this;
    }

    /**
     * Set the execution profile that should be used whenever a statement is executed.
     * This allows for longer timeouts to be set for the execution of the migration scripts
     * as these can take longer than you want your application queries to take.
     *
     * For more information look at <a href="https://docs.datastax.com/en/developer/java-driver/4.6/manual/core/configuration/">Datastax driver configuration</a>.
     *
     * @param executionProfile the name of the execution profile as defined in application.conf. Or null for not using a profile.
     * @return this instance of the <code>MigrationConfiguration</code>. Never null.
     */
    public MigrationConfiguration withExecutionProfile(String executionProfile) {
        this.executionProfile = executionProfile;
        return this;
    }

    /**
     * Returns the table prefix to be used for migration management tables.
     *
     * @return the prefix of the table or <code>EMPTY_TABLE_PREFIX</code> if nothing was configured.
     */
    public String getTablePrefix() {
        return tablePrefix;
    }

    /**
     * Returns the keyspace to be used.
     *
     * @return the configured keyspace or null if <code>isValid()</code> returns false.
     */
    public Keyspace getKeyspace() {
        return keyspace;
    }


    /**
     * Return the name of the configured execution profile.
     *
     * @return name of the execution profile or null if no profile is configured (default).
     */
    public String getExecutionProfile() {
        return this.executionProfile;
    }

    /**
     * Indicates if the underlying configuration is valid. Currently, a configuration is considered
     * valid if a keyspace name or an instance of keyspace is provided.
     *
     * @return true if the configuration is valid, false otherwise.
     */
    public boolean isValid() {
        return keyspace != null;
    }

    @Override
    public String toString() {
        return "MigrationConfiguration {" +
                " [REQUIRED] keyspace=" + keyspace +
                ",[OPTIONAL] tablePrefix='" + tablePrefix + '\'' +
                ",[OPTIONAL] executionProfile='" + executionProfile + '\'' +
                '}';
    }
}
