package org.cognitor.cassandra.migration.keyspace;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.join;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toSet;
import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;

/**
 * @author Patrick Kranz
 */
public class NetworkStrategy implements ReplicationStrategy {
    private final Map<String, Integer> dataCenters = new HashMap<>();

    @Override
    public String getName() {
        return "NetworkTopologyStrategy";
    }

    @Override
    public String createCqlStatement() {
        if (getDataCenters().isEmpty()) {
            throw new IllegalStateException("There has to be at least one datacenter in order to use NetworkTopologyStrategy.");
        }

        return "{" +
                "'class':'" + getName() + "'," +
                join(",",
                        dataCenters.keySet().stream().map(dc -> "'" + dc + "':" + dataCenters.get(dc))
                                .collect(toSet())) +
                "}";
    }

    public NetworkStrategy with(String datacenter, int replicationFactor) {
        notNullOrEmpty(datacenter, "datacenter");
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("Replication Factor must be greater than zero");
        }
        dataCenters.put(datacenter, replicationFactor);
        return this;
    }

    public Map<String, Integer> getDataCenters() {
        return unmodifiableMap(dataCenters);
    }
}
