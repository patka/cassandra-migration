package org.cognitor.cassandra.it.migration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.cognitor.cassandra.CassandraJUnitRule;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Patrick Kranz
 */
public class DatabaseTest {
    @Rule
    public CassandraJUnitRule cassandra = new CassandraJUnitRule("migrationCassandraInit.cql");

    private Database database;

    @Before
    public void setUp() {
        database = new Database(cassandra.getCluster(), "test_keyspace");
    }

    @Test
    public void shouldReturnSchemaVersionOfZeroInAnEmptyDatabase() {
        assertThat(database.getVersion(), is(equalTo(0)));
    }

    @Test
    public void shouldApplyMigrationToDatabaseWhenMigrationsAndEmptyDatabaseGiven() {
        MigrationTask migrationTask = new MigrationTask(database, new MigrationRepository("cassandra/migrationtest"));
        migrationTask.migrate();
        assertThat(database.getVersion(), is(equalTo(2)));
    }

    @Test
    public void shouldNotApplyAnyMigrationWhenDatabaseAndScriptsAreAtSameVersion() {
        // provide a path without scripts to simulate this
        MigrationRepository repository = new MigrationRepository("migrationtest");
        new MigrationTask(database,repository).migrate();

        assertThat(database.getVersion(), is(equalTo(0)));
    }
}
