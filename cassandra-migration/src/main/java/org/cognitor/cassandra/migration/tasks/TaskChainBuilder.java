package org.cognitor.cassandra.migration.tasks;

import com.datastax.driver.core.Cluster;
import org.cognitor.cassandra.migration.Configuration;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationRepository;

import static org.cognitor.cassandra.migration.util.Ensure.notNull;

/**
 * This class takes the configuration and configures the {@link TaskChain}
 * based on the given configuration. Afterwards the {@link TaskChain}
 * can be modified to contain more tasks that are not related to the
 * configuration.
 *
 * @author Patrick Kranz
 */
public class TaskChainBuilder {
    private final Configuration configuration;
    private final Cluster cluster;
    private final MigrationRepository migrationRepository;

    public TaskChainBuilder(Cluster cluster, Configuration configuration) {
        this(cluster, configuration, new MigrationRepository(configuration.getMigrationLocation()));
    }

    public TaskChainBuilder(Cluster cluster, Configuration configuration, MigrationRepository repository) {
        this.cluster = notNull(cluster, "cluster");
        this.configuration = notNull(configuration, "configuration");
        this.migrationRepository = notNull(repository, "repository");
    }

    public TaskChain buildTaskChain() {
        Database database = new Database(cluster, configuration);
        TaskChain chain = new TaskChain();
        if (configuration.isRecalculateChecksumOnly()) {
            return chain.addTask(new RecalculateChecksumTask(database, migrationRepository));
        }

        if (configuration.isValidateOnly()) {
            return chain.addTask(new ChecksumValidationTask(database, migrationRepository));
        }

        if (configuration.isCreateKeyspace()) {
            chain.addTask(new KeyspaceCreationTask(cluster, configuration.getKeyspaceDefinition()));
        }
        if (configuration.isRecalculateChecksum()) {
            chain.addTask(new RecalculateChecksumTask(database, migrationRepository));
        }
        if (configuration.isChecksumValidation() && !configuration.isRecalculateChecksum()) {
            chain.addTask(new ChecksumValidationTask(database, migrationRepository));
        }
        return chain.addTask(new MigrationTask(database, migrationRepository));
    }
}
