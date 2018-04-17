package org.cognitor.cassandra.migration.tasks;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import org.cognitor.cassandra.migration.MigrationException;
import org.cognitor.cassandra.migration.keyspace.KeyspaceDefinition;

import static java.lang.String.format;
import static org.cognitor.cassandra.migration.util.Ensure.notNull;

/**
 * This task creates a keyspace in the cluster based on the given
 * keyspace definition. This task can be executed multiple times.
 * If the keyspace already exists, nothing happens. This task does
 * <b>not</b> update the keyspace.
 *
 * @author Patrick Kranz
 */
public class KeyspaceCreationTask implements Task {
    private final Cluster cluster;
    private final KeyspaceDefinition keyspace;

    public KeyspaceCreationTask(Cluster cluster, KeyspaceDefinition keyspace) {
        this.cluster = notNull(cluster, "cluster");
        this.keyspace = notNull(keyspace, "keyspace");
    }

    @Override
    public void execute() {
        if (keyspaceExists()) {
            return;
        }
        try (Session session = this.cluster.connect()) {
            session.execute(this.keyspace.getCqlStatement());
        } catch (DriverException exception) {
            throw new MigrationException(format("Unable to create keyspace %s.", keyspace.getKeyspaceName()), exception);
        }
    }

    private boolean keyspaceExists() {
        return cluster.getMetadata().getKeyspace(keyspace.getKeyspaceName()) != null;
    }

}
