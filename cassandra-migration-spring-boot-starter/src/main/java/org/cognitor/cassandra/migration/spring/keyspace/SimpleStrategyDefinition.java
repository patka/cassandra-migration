package org.cognitor.cassandra.migration.spring.keyspace;

import org.cognitor.cassandra.migration.keyspace.ReplicationStrategy;
import org.cognitor.cassandra.migration.keyspace.SimpleStrategy;

public class SimpleStrategyDefinition implements KeyspaceReplicationStrategyDefinition {

    private int replicationFactor = 1;

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    @Override
    public ReplicationStrategy getStrategy() {
        return new SimpleStrategy(replicationFactor);
    }
}
