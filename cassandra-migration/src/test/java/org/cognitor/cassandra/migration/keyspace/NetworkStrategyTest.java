package org.cognitor.cassandra.migration.keyspace;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Patrick Kranz
 */
public class NetworkStrategyTest {
    @Test
    public void shouldReturnCqlStringWhenOneDatacenterGiven() {
        String json = new NetworkStrategy().with("EAST", 3).createCqlStatement();
        assertThat(json, is(equalTo("{'class':'NetworkTopologyStrategy','EAST':3}")));
    }

    @Test
    public void shouldReturnCqlStringWhenTwoDatacenterGiven() {
        String json = new NetworkStrategy()
                .with("EAST", 3)
                .with("WEST", 2)
                .createCqlStatement();
        assertThat(json, is(equalTo("{'class':'NetworkTopologyStrategy','EAST':3,'WEST':2}")));
    }

    @Test
    public void shouldThrowExceptionWhenNoDatacenterGiven() {
        assertThrows(IllegalStateException.class, () -> new NetworkStrategy().createCqlStatement());
    }

    @Test
    public void shouldThrowExceptionWhenReplicationFactorLessThenOneGiven() {
        assertThrows(IllegalArgumentException.class, () -> new NetworkStrategy().with("EAST", -1));
    }
}