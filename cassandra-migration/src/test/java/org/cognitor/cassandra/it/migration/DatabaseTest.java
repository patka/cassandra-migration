package org.cognitor.cassandra.it.migration;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationException;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;
import org.cognitor.cassandra.migration.keyspace.Keyspace;
import org.cognitor.cassandra.migration.keyspace.NetworkStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * @author Patrick Kranz
 */
public class DatabaseTest {

    private static final String KEYSPACE = "test_keyspace";
    private static final String NETWORK_KEYSPACE = "network_keyspace";
    private static final String NEW_KEYSPACE = "new_keyspace";
    private static final String CASSANDRA_HOST = "127.0.0.1";
    private static final int REQUEST_TIMEOUT_IN_SECONDS = 30;
    private CqlSession session;

    @Before
    public void before() {
        session = createSession();
        session.execute("CREATE KEYSPACE test_keyspace WITH REPLICATION = " +
                "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    }

    @After
    public void after() {
        if (session.isClosed()) {
            session = createSession();
        }

        for (String keyspace : asList(KEYSPACE, NETWORK_KEYSPACE, NEW_KEYSPACE)) {
            session.execute("DROP KEYSPACE IF EXISTS " + keyspace + ";");
        }
        session.close();
    }

    private CqlSession createSession() {
        DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(REQUEST_TIMEOUT_IN_SECONDS))
                .withBoolean(DefaultDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, false)
                .build();
        return new CqlSessionBuilder()
                .addContactPoint(new InetSocketAddress(CASSANDRA_HOST, 9042))
                .withConfigLoader(loader)
                .withLocalDatacenter("datacenter1")
                .build();
    }

    @Test
    public void shouldReturnSchemaVersionOfZeroInAnEmptyDatabase() {
        Database database = new Database(session, KEYSPACE);
        assertThat(database.getVersion(), is(equalTo(0)));
    }

    @Test
    public void shouldCreateMigrationTableWithPrefixWhenPrefixGiven() {
        new Database(session, KEYSPACE, "prefix");
        assertThat(session.getMetadata().getKeyspace(KEYSPACE).get()
                .getTable("prefix_schema_migration").isPresent(), is(true));
        assertThat(session.getMetadata().getKeyspace(KEYSPACE).get()
                .getTable("schema_migration").isPresent(), is(false));
    }

    @Test
    public void shouldApplyMigrationToDatabaseWhenMigrationsAndPrefixAndEmptyDatabaseGiven() {
        final String prefix = "prefix";
        Database database = new Database(session, KEYSPACE, prefix);
        MigrationTask migrationTask = new MigrationTask(database, new MigrationRepository("cassandra/migrationtest/successful"), false);
        migrationTask.migrate();
        // after migration the database object is closed
        session = createSession();
        database = new Database(session, KEYSPACE, prefix);
        assertThat(database.getVersion(), is(equalTo(3)));

        List<Row> results = loadMigrations(prefix);
        assertThat(results.size(), is(equalTo(3)));
        assertThat(results.get(0).getBoolean("applied_successful"), is(true));
        assertThat(results.get(1).getBoolean("applied_successful"), is(true));
        assertThat(results.get(2).getBoolean("applied_successful"), is(true));
    }

    @Test
    public void shouldApplyMigrationToDatabaseWhenMigrationsAndEmptyDatabaseGiven() {
        Database database = new Database(session, KEYSPACE);
        MigrationTask migrationTask = new MigrationTask(database, new MigrationRepository("cassandra/migrationtest/successful"), false);
        migrationTask.migrate();
        // after migration the database object is closed
        session = createSession();
        database = new Database(session, KEYSPACE);
        assertThat(database.getVersion(), is(equalTo(3)));

        List<Row> results = loadMigrations("");
        assertThat(results.size(), is(equalTo(3)));
        assertThat(results.get(0).getBoolean("applied_successful"), is(true));
        assertThat(results.get(0).getInstant("executed_at"), is(not(nullValue())));
        assertThat(results.get(0).getString("script_name"), is(equalTo("001_init.cql")));
        assertThat(results.get(0).getString("script"), is(startsWith("CREATE TABLE")));
        assertThat(results.get(1).getBoolean("applied_successful"), is(true));
        assertThat(results.get(1).getInstant("executed_at"), is(not(nullValue())));
        assertThat(results.get(1).getString("script_name"), is(equalTo("002_add_events_table.cql")));
        assertThat(results.get(1).getString("script"), is(equalTo("CREATE TABLE EVENTS (event_id uuid primary key, event_name varchar);")));
        assertThat(results.get(2).getBoolean("applied_successful"), is(true));
        assertThat(results.get(2).getInstant("executed_at"), is(not(nullValue())));
        assertThat(results.get(2).getString("script_name"), is(equalTo("003_add_another_table.cql")));
        assertThat(results.get(2).getString("script"), is(equalTo("CREATE TABLE THINGS (thing_id uuid primary key, thing_name varchar);")));
    }

    @Test
    public void shouldApplyConcurrentMigrationsToDatabaseWhenMigrationsAndEmptyDatabaseGiven()
            throws InterruptedException, ExecutionException {
        int concurrentTasks = 3;
        ExecutorService executorService = Executors.newFixedThreadPool(concurrentTasks);
        List<Database> databases = new ArrayList<>();
        List<MigrationTask> migrationTasks = new ArrayList<>();

        for (int i = 0; i < concurrentTasks; i++) {
            databases.add(new Database(createSession(), KEYSPACE));
            migrationTasks.add(
                    new MigrationTask(
                            databases.get(i),
                            new MigrationRepository("cassandra/migrationtest/successful"),
                            true));
        }
        List<Callable<Boolean>> migrations = migrationTasks.stream().map(task -> databaseMigrationTask(task))
                .collect(Collectors.toList());

        // Executing the same migration concurrently with different threads
        List<Future<Boolean>> futures = executorService.invokeAll(migrations);
        for (Future<Boolean> future : futures) {
            future.get();
        }

        Database database = new Database(session, KEYSPACE);
        assertThat(database.getVersion(), is(equalTo(3)));

        List<Row> results = loadMigrations("");
        assertThat(results.size(), is(equalTo(3)));
        assertThat(results.get(0).getBoolean("applied_successful"), is(true));
        assertThat(results.get(0).getInstant("executed_at"), is(not(nullValue())));
        assertThat(results.get(0).getString("script_name"), is(equalTo("001_init.cql")));
        assertThat(results.get(0).getString("script"), is(startsWith("CREATE TABLE")));
        assertThat(results.get(1).getBoolean("applied_successful"), is(true));
        assertThat(results.get(1).getInstant("executed_at"), is(not(nullValue())));
        assertThat(results.get(1).getString("script_name"), is(equalTo("002_add_events_table.cql")));
        assertThat(results.get(1).getString("script"),
                is(equalTo("CREATE TABLE EVENTS (event_id uuid primary key, event_name varchar);")));
        assertThat(results.get(2).getBoolean("applied_successful"), is(true));
        assertThat(results.get(2).getInstant("executed_at"), is(not(nullValue())));
        assertThat(results.get(2).getString("script_name"), is(equalTo("003_add_another_table.cql")));
        assertThat(results.get(2).getString("script"),
                is(equalTo("CREATE TABLE THINGS (thing_id uuid primary key, thing_name varchar);")));
    }

    private Callable<Boolean> databaseMigrationTask(MigrationTask migrationTask) {
        return () -> {
            migrationTask.migrate();
            return true;
        };
    }


    @Test
    public void shouldNotApplyAnyMigrationWhenDatabaseAndScriptsAreAtSameVersion() {
        Database database = new Database(session, KEYSPACE);
        // provide a path without scripts to simulate this
        MigrationRepository repository = new MigrationRepository("migrationtest");
        new MigrationTask(database, repository, false).migrate();

        session = createSession();
        Database openDatabase = new Database(session, KEYSPACE);
        assertThat(openDatabase.getVersion(), is(equalTo(0)));
        assertThat(session.isClosed(), is(false));
    }

    @Test
    public void shouldThrowExceptionAndLogFailedMigrationWhenWrongMigrationScriptGiven() {
        Database database = new Database(session, KEYSPACE);
        MigrationRepository repository = new MigrationRepository("cassandra/migrationtest/failing/brokenstatement");
        MigrationException exception = null;
        try {
            new MigrationTask(database, repository, false).migrate();
        } catch (MigrationException e) {
            exception = e;
        }
        assertThat(exception, is(not(nullValue())));
        assertThat(exception.getMessage(), is(not(nullValue())));
        assertThat(exception.getScriptName(), is(equalTo("001_init.cql")));
        assertThat(exception.getStatement(), is(equalTo("CREATE TABLE PERSON (id uuid primary key, name varcha);")));
        session = createSession();
        session.execute("USE " + KEYSPACE);
        List<Row> results = loadMigrations("");
        assertThat(results.size(), is(equalTo(1)));
        assertThat(results.get(0).getBoolean("applied_successful"), is(false));
        assertThat(results.get(0).getInstant("executed_at"), is(not(nullValue())));
    }

    @Test
    public void shouldCreateKeyspaceWhenDatabaseWithoutKeyspaceAndKeyspaceDefinitionGiven() {
        Database database = new Database(session, KEYSPACE);
        assertThat(session.getMetadata().getKeyspace(NEW_KEYSPACE).isPresent(), is(false));
        Keyspace keyspace = new Keyspace(NEW_KEYSPACE);
        Database db = new Database(session, keyspace);

        KeyspaceMetadata keyspaceMetadata = session.getMetadata().getKeyspace(NEW_KEYSPACE).get();
        assertThat(keyspaceMetadata, is(notNullValue()));
        assertThat(keyspaceMetadata.getReplication().get("class"),
                is(equalTo("org.apache.cassandra.locator.SimpleStrategy")));
        assertThat(keyspaceMetadata.getReplication().get("replication_factor"), is(equalTo("1")));
        assertThat(db.getVersion(), is(equalTo(0)));
    }

    @Test
    public void shouldCreateKeyspaceWhenDatabaseWithoutKeyspaceAndNetworkKeyspaceDefinitionGiven() {
        Database database = new Database(session, KEYSPACE);
        assertThat(session.getMetadata().getKeyspace(NETWORK_KEYSPACE).isPresent(), is(false));
        Keyspace keyspace = new Keyspace(NETWORK_KEYSPACE)
                .with(new NetworkStrategy().with("dc1", 1));
        Database db = new Database(session, keyspace);

        KeyspaceMetadata keyspaceMetadata = session.getMetadata().getKeyspace(NETWORK_KEYSPACE).get();
        assertThat(keyspaceMetadata, is(notNullValue()));
        assertThat(keyspaceMetadata.getReplication().get("class"),
                is(equalTo("org.apache.cassandra.locator.NetworkTopologyStrategy")));
        assertThat(keyspaceMetadata.getReplication().get("dc1"), is(equalTo("1")));
    }

    @Test
    public void shouldCreateFunctionWhenMigrationScriptWithFunctionGiven() {
        Database database = new Database(session, KEYSPACE);
        MigrationTask migrationTask = new MigrationTask(database, new MigrationRepository("cassandra/migrationtest/function"), false);
        migrationTask.migrate();
        session = createSession();
        database = new Database(session, KEYSPACE);
        assertThat(database.getVersion(), is(equalTo(1)));
        assertThat(session.getMetadata()
                .getKeyspace(KEYSPACE).get().getFunctions().size(), is(equalTo(1)));
    }

    private List<Row> loadMigrations(String tablePrefix) {
        if (tablePrefix == null || tablePrefix.isEmpty()) {
            return session.execute(
                    SimpleStatement.newInstance("SELECT * FROM schema_migration;")).all();
        }
        return session.execute(
                SimpleStatement.newInstance(String.format("SELECT * FROM %s_schema_migration;", tablePrefix))).all();
    }

    @Test
    public void supportsMultilineStatements() {
        Database database = new Database(session, KEYSPACE);
        MigrationRepository repository = new MigrationRepository("cassandra/migrationtest/multiline");
        MigrationTask migrationTask = new MigrationTask(database, repository);
        migrationTask.migrate();
    }
}
