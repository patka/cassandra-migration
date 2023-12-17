package org.cognitor.cassandra.migration.keyspace;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Patrick Kranz
 */
public class SimpleStrategyTest {
    @Test
    public void shouldReturnCqlStringWhenValidReplicationFactorGiven() {
        ReplicationStrategy replicationStrategy = new SimpleStrategy(1);
        assertThat(replicationStrategy.createCqlStatement(), is(equalTo(
                "{'class':'SimpleStrategy','replication_factor':1}"
        )));
    }

    @Test
    public void shouldReturnCqlStringWithOneReplicationWhenNoReplicationFactorGiven() {
        ReplicationStrategy replicationStrategy = new SimpleStrategy();
        assertThat(replicationStrategy.createCqlStatement(), is(equalTo(
                "{'class':'SimpleStrategy','replication_factor':1}"
        )));
    }

    @Test
    public void shouldThrowExceptionWhenReplicationFactorSmallerThanOneGiven() {
        assertThrows(IllegalArgumentException.class, () -> new SimpleStrategy(0));
    }
}