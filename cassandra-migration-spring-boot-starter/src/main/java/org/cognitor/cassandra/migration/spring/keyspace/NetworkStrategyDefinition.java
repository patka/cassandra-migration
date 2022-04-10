package org.cognitor.cassandra.migration.spring.keyspace;

import org.cognitor.cassandra.migration.keyspace.NetworkStrategy;
import org.cognitor.cassandra.migration.keyspace.ReplicationStrategy;

import java.util.Map;

public class NetworkStrategyDefinition implements KeyspaceReplicationStrategyDefinition {

    private Map<String, Integer> replications;

    public Map<String, Integer> getReplications() {
        return replications;
    }

    public void setReplications(Map<String, Integer> replications) {
        this.replications = replications;
    }

    @Override
    public ReplicationStrategy getStrategy() {
        NetworkStrategy networkStrategy = new NetworkStrategy();
        replications.forEach(networkStrategy::with);
        return networkStrategy;
    }
}
