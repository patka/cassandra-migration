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
public class Keyspace {
    private final String keyspaceName;
    private boolean durableWrites;
    private ReplicationStrategy replicationStrategy;

    /**
     * This creates a new instance of a keyspace using the provided keyspace name. It by default
     * uses a {@link SimpleStrategy} for replication and sets durable writes to <code>true</code>.
     * These default values can be overwritten by the provided methods.
     *
     * @param keyspaceName the name of the keyspace to be used.
     */
    public Keyspace(String keyspaceName) {
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
     * @return the current Keyspace instance
     */
    public Keyspace withoutDurableWrites() {
        this.durableWrites = false;
        return this;
    }

    public Keyspace with(ReplicationStrategy replicationStrategy) {
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
        StringBuilder builder = new StringBuilder(60);
        builder.append("CREATE KEYSPACE IF NOT EXISTS ")
                .append(getKeyspaceName())
                .append(" WITH REPLICATION = ")
                .append(getReplicationStrategy().createCqlStatement())
                .append(" AND DURABLE_WRITES = ")
                .append(Boolean.toString(isDurableWrites()));
        return builder.toString();
    }
}
