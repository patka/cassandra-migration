package org.cognitor.cassandra.migration.spring;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * Configuration properties for the cassandra migration library.
 * These values should be set a properties file that is used inside the
 * application, e.g. application.properties.
 *
 * @author Patrick Kranz
 */
@ConfigurationProperties(prefix = "cassandra.migration")
public class CassandraMigrationConfigurationProperties {
    private ScriptCollectorStrategy strategy = ScriptCollectorStrategy.FAIL_ON_DUPLICATES;
    private String scriptLocation = MigrationRepository.DEFAULT_SCRIPT_PATH;
    private String tablePrefix = "";
    private String executionProfileName = null;
    private DefaultConsistencyLevel consistencyLevel = DefaultConsistencyLevel.QUORUM;
    private Boolean withConsensus = false;
    private KeyspaceProperties keyspace = new KeyspaceProperties();

    /**
     * Configuration properties for the keyspace.
     *
     * @author rbleuse
     */
    public static class KeyspaceProperties {
        private String keyspaceName;
        private KeyspaceReplicationStrategy replicationStrategy = KeyspaceReplicationStrategy.SIMPLE;
        private Map<String, Integer> replications;

        /**
         * @return true if a keyspace name was provided, false otherwise
         */
        public boolean hasKeyspaceName() {
            return this.keyspaceName != null && !this.keyspaceName.isEmpty();
        }

        /**
         * @return the name of the keyspace. Can be null if it was not set
         *          before.
         */
        public String getKeyspaceName() {
            return keyspaceName;
        }

        /**
         * Sets the name of the keyspace that should be migrated. This
         * setting is required in order for the migration to work.
         *
         * @param keyspaceName the name of the keyspace to be migrated
         */
        public void setKeyspaceName(String keyspaceName) {
            this.keyspaceName = keyspaceName;
        }

        /**
         * @return the strategy to use when creating the keyspace if required.
         */
        public KeyspaceReplicationStrategy getReplicationStrategy() {
            return replicationStrategy;
        }

        /**
         * Sets the strategy that should be used when creating the keyspace if required.
         *
         * @param replicationStrategy the replication strategy. This setting is optional.
         */
        public void setReplicationStrategy(KeyspaceReplicationStrategy replicationStrategy) {
            this.replicationStrategy = replicationStrategy;
        }

        /**
         * @return the keyspace replication properties.
         * Can be null if replication strategy is SIMPLE.
         */
        public Map<String, Integer> getReplications() {
            return replications;
        }

        /**
         * Sets the keyspace replications properties.
         *
         * @param replications the replication list to use when creating the keyspace.
         * This setting is optional.
         */
        public void setReplications(Map<String, Integer> replications) {
            this.replications = replications;
        }
    }

    /**
     * @return The location of the migration scripts. Never null.
     */
    public String getScriptLocation() {
        return scriptLocation;
    }

    /**
     * The location where the scripts reside on the classpath.
     * The default is <code>MigrationRepository.DEFAULT_SCRIPT_PATH</code> which
     * points to <code>cassandra/migration</code> on the classpath.
     *
     * @param scriptLocation the location of the migration scripts. Must not be null.
     * @throws IllegalArgumentException when scriptLocation is null or empty
     */
    public void setScriptLocation(String scriptLocation) {
        if (scriptLocation == null || scriptLocation.isEmpty()) {
            throw new IllegalArgumentException("Script location cannot be unset.");
        }
        this.scriptLocation = scriptLocation;
    }

    /**
     * Sets the name of the keyspace that should be migrated. This
     * setting is required in order for the migration to work.
     * Sets the keyspace properties. This setting is required in order for the migration to work.
     * @deprecated Use {@link #setKeyspace(KeyspaceProperties)} instead.
     *
     * @param keyspaceName the name of the keyspace to be migrated
     */
    @Deprecated
    public void setKeyspaceName(String keyspaceName) {
        this.keyspace.setKeyspaceName(keyspaceName);
    }

    /**
     * @return the keyspace properties. Can be null if it was not set
     *          before.
     */
    public KeyspaceProperties getKeyspace() {
        return keyspace;
    }

    /**
     * Sets the keyspace properties. This setting is required in order for the migration to work.
     *
     * @param keyspace the keyspace properties
     */
    public void setKeyspace(KeyspaceProperties keyspace) {
        this.keyspace = keyspace;
    }

    /**
     * @return the strategy to use for collecting scripts inside the repository.
     */
    public ScriptCollectorStrategy getStrategy() {
        return strategy;
    }

    /**
     * Sets the strategy that should be used when scripts are collected
     * from the repository.
     *
     * @param strategy the collector strategy. This setting is optional.
     */
    public void setStrategy(ScriptCollectorStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * @return the consistency level to be used for schema migrations
     */
    public DefaultConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * Sets the consistency level which should be used to execute the schema migrations.
     * Default is <code>ConsistencyLevel.QUORUM</code>
     *
     * @param consistencyLevel the consistency level to be used for migrations
     */
    public void setConsistencyLevel(DefaultConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public CassandraMigrationConfigurationProperties setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
        return this;
    }

    public Boolean isWithConsensus() {
        return withConsensus;
    }

    /**
     * Sets whether or not the migration should use consensus to prevent
     * concurrent schema updates.
     *
     * @param withConsensus enable/disable leader election for migrations
     */
    public void setWithConsensus(Boolean withConsensus) {
        this.withConsensus = withConsensus;
    }

    /**
     * Sets execution profile name which should be used to execute schema migrations.
     * If not set, default for <code>CqlSession</code> is used.
     *
     * @param executionProfileName to be used for migrations
     */
    public CassandraMigrationConfigurationProperties setExecutionProfileName(String executionProfileName) {
        this.executionProfileName = executionProfileName;
        return this;
    }

    /**
     * @return execution profile name or null if default should be used to execute schema migrations.
     */
    public String getExecutionProfileName() {
        return executionProfileName;
    }

}
