package org.cognitor.cassandra.it.migration;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.cognitor.cassandra.CassandraJUnitRule;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationException;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.cognitor.cassandra.CassandraJUnitRule.DEFAULT_SCRIPT_LOCATION;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author Patrick Kranz
 */
public class DatabaseTest {

    @Rule
    public final CassandraJUnitRule cassandra = new CassandraJUnitRule(DEFAULT_SCRIPT_LOCATION, "cassandra.yml");

    private Database database;

    @Before
    public void setUp() {
        database = new Database(cassandra.getCluster(), CassandraJUnitRule.TEST_KEYSPACE);
    }

    @Test
    public void shouldReturnSchemaVersionOfZeroInAnEmptyDatabase() {
        assertThat(database.getVersion(), is(equalTo(0)));
    }

    @Test
    public void shouldApplyMigrationToDatabaseWhenMigrationsAndEmptyDatabaseGiven() {
        MigrationTask migrationTask = new MigrationTask(database, new MigrationRepository("cassandra/migrationtest/successful"));
        migrationTask.migrate();
        // after migration the database object is closed
        database = new Database(cassandra.getCluster(), CassandraJUnitRule.TEST_KEYSPACE);
        assertThat(database.getVersion(), is(equalTo(2)));

        List<Row> results = loadMigrations();
        assertThat(results.size(), is(equalTo(2)));
        assertThat(results.get(0).getBool("applied_successful"), is(true));
        assertThat(results.get(0).getTimestamp("executed_at"), is(not(nullValue())));
        assertThat(results.get(0).getString("script_name"), is(equalTo("001_init.cql")));
        assertThat(results.get(0).getString("script"), is(startsWith("CREATE TABLE")));
        assertThat(results.get(1).getBool("applied_successful"), is(true));
        assertThat(results.get(1).getTimestamp("executed_at"), is(not(nullValue())));
        assertThat(results.get(1).getString("script_name"), is(equalTo("002_add_events_table.cql")));
        assertThat(results.get(1).getString("script"), is(equalTo("CREATE TABLE EVENTS (event_id uuid primary key, event_name varchar);")));
    }


    @Test
    public void shouldNotApplyAnyMigrationWhenDatabaseAndScriptsAreAtSameVersion() {
        // provide a path without scripts to simulate this
        MigrationRepository repository = new MigrationRepository("migrationtest");
        new MigrationTask(database, repository).migrate();

        assertThat(database.getVersion(), is(equalTo(0)));
    }

    @Test
    public void shouldThrowExceptionAndLogFailedMigrationWhenWrongMigrationScriptGiven() {
        MigrationRepository repository = new MigrationRepository("cassandra/migrationtest/failing/brokenstatement");
        MigrationException exception = null;
        try {
            new MigrationTask(database, repository).migrate();
        } catch (MigrationException e) {
            exception = e;
        }
        assertThat(exception, is(not(nullValue())));
        assertThat(exception.getMessage(), is(not(nullValue())));
        assertThat(exception.getScriptName(), is(equalTo("001_init.cql")));
        assertThat(exception.getStatement(), is(equalTo("CREATE TABLE PERSON (id uuid primary key, name varcha)")));

        List<Row> results = loadMigrations();
        assertThat(results.size(), is(equalTo(1)));
        assertThat(results.get(0).getBool("applied_successful"), is(false));
        assertThat(results.get(0).getTimestamp("executed_at"), is(not(nullValue())));
    }

    private List<Row> loadMigrations() {
        Session session = cassandra.getCluster().connect(CassandraJUnitRule.TEST_KEYSPACE);
        ResultSet resultSet = session.execute(new SimpleStatement("SELECT * FROM schema_migration;"));
        return resultSet.all();
    }
}
