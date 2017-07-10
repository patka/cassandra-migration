package org.cognitor.cassandra.migration.keyspace;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

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

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenNoDatacenterGiven() {
        String json = new NetworkStrategy()
                .createCqlStatement();
        assertThat(json, is(equalTo("{'class':'NetworkTopologyStrategy'}")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenReplicatonFactorLessThenOneGiven() {
        new NetworkStrategy().with("EAST", -1);
    }
}