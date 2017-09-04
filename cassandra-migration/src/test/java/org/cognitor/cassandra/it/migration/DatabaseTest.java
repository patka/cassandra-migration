package org.cognitor.cassandra.it.migration;

import com.datastax.driver.core.*;
import org.cognitor.cassandra.CassandraJUnitRule;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationException;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;
import org.cognitor.cassandra.migration.keyspace.Keyspace;
import org.cognitor.cassandra.migration.keyspace.NetworkStrategy;
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

    private static final String SUCCESSFUL_MIGRATIONS = "cassandra/migrationtest/successful";
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
        assertThat(database.getVersion(), is(equalTo(3)));

        List<Row> results = loadMigrations();
        assertThat(results.size(), is(equalTo(3)));
        assertThat(results.get(0).getBool("applied_successful"), is(true));
        assertThat(results.get(0).getTimestamp("executed_at"), is(not(nullValue())));
        assertThat(results.get(0).getString("script_name"), is(equalTo("001_init.cql")));
        assertThat(results.get(0).getString("script"), is(startsWith("CREATE TABLE")));
        assertThat(results.get(1).getBool("applied_successful"), is(true));
        assertThat(results.get(1).getTimestamp("executed_at"), is(not(nullValue())));
        assertThat(results.get(1).getString("script_name"), is(equalTo("002_add_events_table.cql")));
        assertThat(results.get(1).getString("script"), is(equalTo("CREATE TABLE EVENTS (event_id uuid primary key, event_name varchar);")));
        assertThat(results.get(2).getBool("applied_successful"), is(true));
        assertThat(results.get(2).getTimestamp("executed_at"), is(not(nullValue())));
        assertThat(results.get(2).getString("script_name"), is(equalTo("003_add_another_table.cql")));
        assertThat(results.get(2).getString("script"), is(equalTo("CREATE TABLE THINGS (thing_id uuid primary key, thing_name varchar);")));
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

    @Test
    public void shouldCreateKeyspaceWhenDatabaseWithoutKeyspaceAndKeyspaceDefinitionGiven() {
        assertThat(cassandra.getCluster().getMetadata().getKeyspace("new_keyspace"), is(nullValue()));
        Keyspace keyspace = new Keyspace("new_keyspace");
        Database db = new Database(cassandra.getCluster(), keyspace);

        KeyspaceMetadata keyspaceMetadata = cassandra.getCluster().getMetadata().getKeyspace("new_keyspace");
        assertThat(keyspaceMetadata, is(notNullValue()));
        assertThat(keyspaceMetadata.getReplication().get("class"),
                is(equalTo("org.apache.cassandra.locator.SimpleStrategy")));
        assertThat(keyspaceMetadata.getReplication().get("replication_factor"), is(equalTo("1")));
        assertThat(db.getVersion(), is(equalTo(0)));
    }

    @Test
    public void shouldCreateKeyspaceWhenDatabaseWithoutKeyspaceAndNetworkKeyspaceDefinitionGiven() {
        assertThat(cassandra.getCluster().getMetadata().getKeyspace("network_keyspace"), is(nullValue()));
        Keyspace keyspace = new Keyspace("network_keyspace")
                .with(new NetworkStrategy().with("dc1", 1));
        Database db = new Database(cassandra.getCluster(), keyspace);

        KeyspaceMetadata keyspaceMetadata = cassandra.getCluster().getMetadata().getKeyspace("network_keyspace");
        assertThat(keyspaceMetadata, is(notNullValue()));
        assertThat(keyspaceMetadata.getReplication().get("class"),
                is(equalTo("org.apache.cassandra.locator.NetworkTopologyStrategy")));
        assertThat(keyspaceMetadata.getReplication().get("dc1"), is(equalTo("1")));
    }

    private List<Row> loadMigrations() {
        Session session = cassandra.getCluster().connect(CassandraJUnitRule.TEST_KEYSPACE);
        ResultSet resultSet = session.execute(new SimpleStatement("SELECT * FROM schema_migration;"));
        return resultSet.all();
    }
}
