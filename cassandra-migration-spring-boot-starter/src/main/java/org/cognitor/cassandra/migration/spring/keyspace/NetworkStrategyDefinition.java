package org.cognitor.cassandra.migration.spring.keyspace;

import org.cognitor.cassandra.migration.keyspace.NetworkStrategy;
import org.cognitor.cassandra.migration.keyspace.ReplicationStrategy;

import java.util.Map;

public class NetworkStrategyDefinition implements StrategyDefinition {

    private Map<String, Integer> dataCenters;

    public Map<String, Integer> getDataCenters() {
        return dataCenters;
    }

    public void setDataCenters(Map<String, Integer> dataCenters) {
        this.dataCenters = dataCenters;
    }

    @Override
    public ReplicationStrategy getStrategy() {
        NetworkStrategy networkStrategy = new NetworkStrategy();
        dataCenters.forEach(networkStrategy::with);
        return networkStrategy;
    }
}
