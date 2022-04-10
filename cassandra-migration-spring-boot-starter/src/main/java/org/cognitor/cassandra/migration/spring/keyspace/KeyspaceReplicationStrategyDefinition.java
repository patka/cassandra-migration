package org.cognitor.cassandra.migration.spring.keyspace;

import org.cognitor.cassandra.migration.keyspace.ReplicationStrategy;

public interface KeyspaceReplicationStrategyDefinition {

    ReplicationStrategy getStrategy();
}
