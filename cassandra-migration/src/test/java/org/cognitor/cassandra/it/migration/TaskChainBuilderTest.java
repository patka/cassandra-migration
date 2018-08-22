package org.cognitor.cassandra.it.migration;

import org.cognitor.cassandra.CassandraJUnitRule;
import org.cognitor.cassandra.migration.Configuration;
import org.cognitor.cassandra.migration.tasks.*;
import org.junit.Rule;
import org.junit.Test;

import static org.cognitor.cassandra.CassandraJUnitRule.DEFAULT_SCRIPT_LOCATION;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Patrick Kranz
 */
public class TaskChainBuilderTest {
    @Rule
    public final CassandraJUnitRule cassandra = new CassandraJUnitRule(DEFAULT_SCRIPT_LOCATION, "cassandra.yml");

    @Test
    public void shouldReturnRecalculateChecksumTaskWhenRecalculateChecksumOnlyFlagGiven() {
        Configuration config = new Configuration("test_keyspace").setRecalculateChecksumOnly(true);
        TaskChain chain = createTaskChain(config);
        assertThat(chain.getTasks().size(), is(equalTo(1)));
        assertThat(chain.getTasks().get(0).getClass().isAssignableFrom(RecalculateChecksumTask.class), is(true));
    }

    @Test
    public void shouldReturnChecksumValidationTaskWhenChecksumValidationFlagGiven() {
        Configuration config = new Configuration("test_keyspace").setChecksumValidation(true);
        TaskChain chain = createTaskChain(config);
        assertThat(chain.getTasks().size(), is(equalTo(2)));
        assertThat(chain.getTasks().get(0).getClass().isAssignableFrom(ChecksumValidationTask.class), is(true));
        assertThat(chain.getTasks().get(1).getClass().isAssignableFrom(MigrationTask.class), is(true));
    }

    @Test
    public void shouldReturnRecalculateCheckumsAndMigrationTaskWhenFlagRecalculateChecksumGiven() {
        Configuration config = new Configuration("test_keyspace").setRecalculateChecksum(true);
        TaskChain chain = createTaskChain(config);
        assertThat(chain.getTasks().size(), is(equalTo(2)));
        assertThat(chain.getTasks().get(0).getClass().isAssignableFrom(RecalculateChecksumTask.class), is(true));
        assertThat(chain.getTasks().get(1).getClass().isAssignableFrom(MigrationTask.class), is(true));
    }

    @Test
    public void shouldReturnMigrationAndValidationTaskWhenDefaultConfigurationGiven() {
        Configuration config = new Configuration("test_keyspace");
        TaskChain chain = createTaskChain(config);
        assertThat(chain.getTasks().size(), is(equalTo(2)));
        assertThat(chain.getTasks().get(0).getClass().isAssignableFrom(ChecksumValidationTask.class), is(true));
        assertThat(chain.getTasks().get(1).getClass().isAssignableFrom(MigrationTask.class), is(true));
    }

    @Test
    public void shouldReturnChecksumValidationTaskWhenValidationOnlyGiven() {
        Configuration config = new Configuration("test_keyspace").setValidateOnly(true);
        TaskChain chain = createTaskChain(config);
        assertThat(chain.getTasks().size(), is(equalTo(1)));
        assertThat(chain.getTasks().get(0).getClass().isAssignableFrom(ChecksumValidationTask.class), is(true));
    }

    @Test
    public void shouldReturnKeyspaceCreationAndMigrationTaskWhenKeyspaceGiven() {
        Configuration config = new Configuration("test_keyspace").setCreateKeyspace(true);
        TaskChain chain = createTaskChain(config);
        assertThat(chain.getTasks().size(), is(equalTo(3)));
        assertThat(chain.getTasks().get(0).getClass().isAssignableFrom(KeyspaceCreationTask.class), is(true));
        assertThat(chain.getTasks().get(1).getClass().isAssignableFrom(ChecksumValidationTask.class), is(true));
        assertThat(chain.getTasks().get(2).getClass().isAssignableFrom(MigrationTask.class), is(true));
    }

    private TaskChain createTaskChain(Configuration config) {
        TaskChainBuilder builder = new TaskChainBuilder(cassandra.getCluster(), config);
        return builder.buildTaskChain();
    }
}
