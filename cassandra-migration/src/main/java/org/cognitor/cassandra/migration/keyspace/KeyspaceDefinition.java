package org.cognitor.cassandra.migration.keyspace;

import static org.cognitor.cassandra.migration.util.Ensure.notNull;
import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;

/**
 * This represents the definition of a key space and is basically
 * a builder for the CQL statement that is required to create a keyspace
 * before any migration can be executed.
 *
 * @author Patrick Kranz
 */
public class KeyspaceDefinition {
    private final String keyspaceName;
    private boolean durableWrites;
    private ReplicationStrategy replicationStrategy;

    public KeyspaceDefinition(String keyspaceName) {
        this.keyspaceName = notNullOrEmpty(keyspaceName, "keyspaceName");
        this.replicationStrategy = new SimpleStrategy();
        this.durableWrites = true;
    }

    public String getKeyspaceName() {
        return keyspaceName;
    }

    /**
     * Sets durable writes to false and by this bypass
     * the commit log for write operations in this keyspace.
     * This behavior is not recommended and should not be done
     * with SimpleStrategy replication.
     *
     * @return the current KeyspaceDefinition instance
     */
    public KeyspaceDefinition withoutDurableWrites() {
        this.durableWrites = false;
        return this;
    }

    public KeyspaceDefinition with(ReplicationStrategy replicationStrategy) {
        this.replicationStrategy = notNull(replicationStrategy, "replicationStrategy");
        return this;
    }

    public boolean isDurableWrites() {
        return durableWrites;
    }

    public ReplicationStrategy getReplicationStrategy() {
        return replicationStrategy;
    }

    public String getCqlStatement() {
        return "CREATE KEYSPACE IF NOT EXISTS " +
                getKeyspaceName() +
                " WITH REPLICATION = " +
                getReplicationStrategy().createCqlStatement() +
                " AND DURABLE_WRITES = " +
                Boolean.toString(isDurableWrites());
    }
}
