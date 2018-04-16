package org.cognitor.cassandra.migration;

import com.datastax.driver.core.Cluster;
import org.cognitor.cassandra.migration.tasks.MigrationTask;
import org.cognitor.cassandra.migration.tasks.TaskChain;

import static org.cognitor.cassandra.migration.util.Ensure.notNull;

/**
 * @author Patrick Kranz
 */
public class MigrationProcess {
    private final Configuration configuration;
    private final Cluster cluster;
    private final MigrationRepository migrationRepository;

    public MigrationProcess(Cluster cluster, Configuration configuration) {
        this(cluster, configuration, new MigrationRepository(configuration.getMigrationLocation()));
    }

    public MigrationProcess(Cluster cluster, Configuration configuration, MigrationRepository repository) {
        this.cluster = notNull(cluster, "cluster");
        this.configuration = notNull(configuration, "configuration");
        this.migrationRepository = notNull(repository, "repository");
    }

    public void migrate() {
        Database database = new Database(cluster, configuration);
        TaskChain chain = new TaskChain();
        chain.addTask(new MigrationTask(database, migrationRepository));
        chain.execute();
    }
}
