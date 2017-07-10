package org.cognitor.cassandra.migration.keyspace;

/**
 * @author Patrick Kranz
 */
public class SimpleStrategy implements ReplicationStrategy {
    private int replicationFactor;

    public SimpleStrategy(int replicationFactor) {
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("Replication Factor must be greater than zero");
        }
        this.replicationFactor = replicationFactor;
    }

    public SimpleStrategy() {
        this(1);
    }

    @Override
    public String getName() {
        return "SimpleStrategy";
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    @Override
    public String createCqlStatement() {
        return "{" +
                "'class':'" + getName() + "'," +
                "'replication_factor':" + getReplicationFactor() +
                "}";
    }
}
