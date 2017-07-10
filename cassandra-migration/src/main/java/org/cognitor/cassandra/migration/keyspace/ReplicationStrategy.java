package org.cognitor.cassandra.migration.keyspace;

/**
 * @author Patrick Kranz
 */
public interface ReplicationStrategy {
    String getName();
    String createCqlStatement();

}
