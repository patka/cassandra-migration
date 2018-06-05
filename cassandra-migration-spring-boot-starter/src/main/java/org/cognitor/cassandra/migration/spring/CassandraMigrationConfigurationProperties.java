package org.cognitor.cassandra.migration.spring;

import org.cognitor.cassandra.migration.MigrationRepository;
import org.springframework.boot.context.properties.ConfigurationProperties;

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
    private String keyspaceName;
    private boolean checksumValidation = true;
    private boolean checksumValidationOnly = false;

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
     * @return true if a keyspace name was provided, false otherwise
     */
    public boolean hasKeyspaceName() {
        return this.keyspaceName != null && !this.keyspaceName.isEmpty();
    }

    /**
     * @return true if the existing checksums should be validated.
     */
    public boolean isChecksumValidation() {
        return checksumValidation;
    }

    /**
     * Sets if the checksum of existing scripts should be checked before any migration.
     *
     * @param checksumValidation true if checksums should be validated (default)
     */
    public void setChecksumValidation(boolean checksumValidation) {
        this.checksumValidation = checksumValidation;
    }

    /**
     * Returns if the checksum should be validated only. In this case nothing else will be done.
     *
     * @return true if checksum only validation is enabled, false otherwise.
     */
    public boolean isChecksumValidationOnly() {
        return checksumValidationOnly;
    }

    /**
     * Sets if the checksum should be validated only. In this case nothing else will be done.
     *
     * @param checksumValidationOnly true if checksum only validation is enabled, false otherwise.
     */
    public CassandraMigrationConfigurationProperties setChecksumValidationOnly(boolean checksumValidationOnly) {
        this.checksumValidationOnly = checksumValidationOnly;
        return this;
    }
}
