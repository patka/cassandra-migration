package org.cognitor.cassandra.migration.spring;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.keyspace.ReplicationStrategy;
import org.cognitor.cassandra.migration.spring.keyspace.KeyspaceReplicationStrategyDefinition;
import org.cognitor.cassandra.migration.spring.keyspace.NetworkStrategyDefinition;
import org.cognitor.cassandra.migration.spring.keyspace.SimpleStrategyDefinition;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;

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
    private List<String> scriptLocations = singletonList(MigrationRepository.DEFAULT_SCRIPT_PATH);
    private String keyspaceName;
    private KeyspaceReplicationStrategyDefinition simpleStrategy = new SimpleStrategyDefinition();
    private KeyspaceReplicationStrategyDefinition networkStrategy;
    private String tablePrefix = "";
    private String executionProfileName = null;
    private DefaultConsistencyLevel consistencyLevel = DefaultConsistencyLevel.QUORUM;
    private Boolean withConsensus = false;

    /**
     * This method is deprecated in favor of <code>getScriptLocations</code> and returns the first
     * element of the list that was set. If there is only one element this behaves the same way
     * it always did. This method is here for backward compatibility only and will be removed in the
     * next major release.
     *
     * @return The location of the migration scripts. Never null.
     */
    @Deprecated
    public String getScriptLocation() {
        return this.scriptLocations.get(0);
    }

    /**
     * The location where the scripts reside on the classpath.
     * The default is <code>MigrationRepository.DEFAULT_SCRIPT_PATH</code> which
     * points to <code>cassandra/migration</code> on the classpath.
     * @deprecated Use {@link #setScriptLocations(List)} instead.
     *
     * @param scriptLocation the location of the migration scripts. Must not be null.
     * @throws IllegalArgumentException when scriptLocation is null or empty
     */
    @Deprecated
    public void setScriptLocation(String scriptLocation) {
        this.scriptLocations = singletonList(notNullOrEmpty(scriptLocation, "scriptLocation"));
    }

    /**
     * @return The locations of the migration scripts. Never null.
     */
    public List<String> getScriptLocations() {
        return Collections.unmodifiableList(scriptLocations);
    }

    /**
     * The location where the scripts reside on the classpath.
     * The default is <code>MigrationRepository.DEFAULT_SCRIPT_PATH</code> which
     * points to <code>cassandra/migration</code> on the classpath.
     *
     * @param scriptLocations the locations of the migration scripts. Must not be null nor empty.
     * @throws IllegalArgumentException when scriptLocation is null or empty
     */
    public void setScriptLocations(List<String> scriptLocations) {
        this.scriptLocations = notNullOrEmpty(scriptLocations, "scriptLocations");
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
     * @return true if a keyspace name was provided, false otherwise
     */
    public boolean hasKeyspaceName() {
        return this.keyspaceName != null && !this.keyspaceName.isEmpty();
    }

    /**
     * @return the strategy to use for creating keyspace. network strategy if is present,
     * simple strategy otherwise.
     */
    public ReplicationStrategy getReplicationStrategy() {
        if (networkStrategy == null) {
            return simpleStrategy.getStrategy();
        }

        return networkStrategy.getStrategy();
    }

    /**
     * Sets the simple replication strategy that should be used to create keyspace
     *
     * @param simpleStrategy the simple strategy. This setting is optional.
     */
    public void setSimpleStrategy(SimpleStrategyDefinition simpleStrategy) {
        this.simpleStrategy = simpleStrategy;
    }

    /**
     * Sets the network replication strategy that should be used to create keyspace
     *
     * @param networkStrategy the network strategy. This setting is optional.
     */
    public void setNetworkStrategy(NetworkStrategyDefinition networkStrategy) {
        this.networkStrategy = networkStrategy;
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
