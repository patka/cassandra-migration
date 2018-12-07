package org.cognitor.cassandra.migration.spring.keyspace;

import org.cognitor.cassandra.migration.keyspace.ReplicationStrategy;
import org.cognitor.cassandra.migration.keyspace.SimpleStrategy;

public class SimpleStrategyDefinition implements StrategyDefinition {

    private int replicationFactory;

    public SimpleStrategyDefinition(int replicationFactory) {
        this.replicationFactory = replicationFactory;
    }

    public int getReplicationFactory() {
        return replicationFactory;
    }

    public void setReplicationFactory(int replicationFactory) {
        this.replicationFactory = replicationFactory;
    }

    public ReplicationStrategy getStrategy() {
        return new SimpleStrategy(replicationFactory);
    }
}
