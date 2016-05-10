package org.cognitor.cassandra.it.migration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

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

/**
 * @author Patrick Kranz
 */
public class DatabaseTest {
    private static final String KEYSPACE = "test_keyspace";

    @Rule
    public CassandraJUnitRule cassandra = new CassandraJUnitRule("migrationCassandraInit.cql");

    private Database database;

    @Before
    public void setUp() {
        database = new Database(cassandra.getCluster(), KEYSPACE);
    }

    @Test
    public void shouldReturnSchemaVersionOfZeroInAnEmptyDatabase() {
        assertThat(database.getVersion(), is(equalTo(0)));
    }

    @Test
    public void shouldApplyMigrationToDatabaseWhenMigrationsAndEmptyDatabaseGiven() {
        MigrationTask migrationTask = new MigrationTask(database, new MigrationRepository("cassandra/migrationtest/successful"));
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

        Session session = cassandra.getCluster().connect(KEYSPACE);
        ResultSet resultSet = session.execute(new SimpleStatement("SELECT * FROM schema_migration;"));
        List<Row> results = resultSet.all();
        assertThat(results.size(), is(equalTo(1)));
        assertThat(results.get(0).getBool("applied_successful"), is(false));
    }
}
