package org.cognitor.cassandra.migration.keyspace;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Patrick Kranz
 */
public class KeyspaceTest {
    @Test
    public void shouldCreateCqlStatementWhenNameAndStrategyGiven() {
        Keyspace keyspace = new Keyspace("test").with(new NetworkStrategy().with("dc1", 2));
        assertThat(keyspace.getCqlStatement(), is(equalTo(
                "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class':'NetworkTopologyStrategy','dc1':2} " +
                        "AND DURABLE_WRITES = true"
        )));
    }

    @Test
    public void shouldCreateCqlStatementWithSimpleStrategyWhenNoStrategyGiven() {
        Keyspace keyspace = new Keyspace("test");
        assertThat(keyspace.getCqlStatement(), is(equalTo(
                "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1} " +
                        "AND DURABLE_WRITES = true"
        )));
    }

    @Test
    public void shouldCreateCqlWithoutDurableWritesWhenDurableWritesTurnedOffGiven() {
        Keyspace keyspace = new Keyspace("test").withoutDurableWrites();
        assertThat(keyspace.getCqlStatement(), is(equalTo(
                "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1} " +
                        "AND DURABLE_WRITES = false"
        )));
    }

}