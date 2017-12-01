package org.cognitor.cassandra.migration.keyspace;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Patrick Kranz
 */
public class KeyspaceDefinitionTest {
    @Test
    public void shouldCreateCqlStatementWhenNameAndStrategyGiven() {
        KeyspaceDefinition keyspace = new KeyspaceDefinition("test").with(new NetworkStrategy().with("dc1", 2));
        assertThat(keyspace.getCqlStatement(), is(equalTo(
                "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class':'NetworkTopologyStrategy','dc1':2} " +
                        "AND DURABLE_WRITES = true"
        )));
    }

    @Test
    public void shouldCreateCqlStatementWithSimpleStrategyWhenNoStrategyGiven() {
        KeyspaceDefinition keyspace = new KeyspaceDefinition("test");
        assertThat(keyspace.getCqlStatement(), is(equalTo(
                "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1} " +
                        "AND DURABLE_WRITES = true"
        )));
    }

    @Test
    public void shouldCreateCqlWithoutDurableWritesWhenDurableWritesTurnedOffGiven() {
        KeyspaceDefinition keyspace = new KeyspaceDefinition("test").withoutDurableWrites();
        assertThat(keyspace.getCqlStatement(), is(equalTo(
                "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1} " +
                        "AND DURABLE_WRITES = false"
        )));
    }

}