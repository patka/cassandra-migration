package org.cognitor.cassandra.migration.spring;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import org.cognitor.cassandra.migration.MigrationTask;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.*;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

import static org.cognitor.cassandra.migration.spring.CassandraMigrationAutoConfiguration.CQL_SESSION_BEAN_NAME;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author Patrick Kranz
 */
public class CassandraMigrationAutoConfigurationTest {
    private static final String KEYSPACE = "test_keyspace";
    private static Cassandra cassandra;
    private static CqlSession session;

    @BeforeClass
    public static void beforeClass() {
        cassandra = new CassandraBuilder()
                .version("3.11.12")
                .build();
        cassandra.start();
        session = createSession();
        dropKeyspaceIfExists();
    }

    @After
    public void after() {
        dropKeyspaceIfExists();
    }

    @AfterClass
    public static void stopDb() {
        session.close();
        if (null != cassandra) {
            cassandra.stop();
        }
    }

    private static void createKeyspace() {
        session.execute("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION = " +
                "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    }

    private static void dropKeyspaceIfExists() {
        session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE + ";");
    }

    @Test
    public void shouldMigrateDatabaseWhenClusterGivenWithoutAutoKeyspaceCreation() {
        // CREATE KEYSPACE MANUALLY
        createKeyspace();

        // GIVEN
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        TestPropertyValues testValues = TestPropertyValues.of("cassandra.migration.keyspace-name:" + KEYSPACE,
                "cassandra.migration.script-location:cassandra/migration");
        testValues.applyTo(context);
        context.register(ClusterConfig.class, CassandraMigrationAutoConfiguration.class);
        context.refresh();
        // WHEN
        context.getBean(MigrationTask.class);

        // THEN
        CqlSession session = createSession();
        List<Row> rows = session.execute("SELECT * FROM " + KEYSPACE + ".schema_migration").all();
        assertThat(rows.size(), is(equalTo(1)));
        Row migration = rows.get(0);
        assertThat(migration.getBoolean("applied_successful"), is(true));
        assertThat(migration.getInstant("executed_at"), is(not(nullValue())));
        assertThat(migration.getString("script_name"), is(CoreMatchers.equalTo("001_create_person_table.cql")));
        assertThat(migration.getString("script"), startsWith("CREATE TABLE"));
        session.close();
    }

    @Test
    public void shouldMigrateDatabaseWhenClusterGivenWithMultipleLocationsWithoutAutoKeyspaceCreation() {
        // GIVEN
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        TestPropertyValues testValues = TestPropertyValues.of("cassandra.migration.keyspace-name:" + KEYSPACE,
                "cassandra.migration.script-locations:cassandra/dev,cassandra/common");
        testValues.applyTo(context);
        context.register(ClusterConfig.class, CassandraMigrationAutoConfiguration.class);
        context.refresh();
        // WHEN
        context.getBean(MigrationTask.class);

        // THEN
        CqlSession session = createSession();
        List<Row> rows = session.execute("SELECT * FROM " + KEYSPACE + ".schema_migration").all();
        assertThat(rows.size(), is(equalTo(2)));
        Row migration = rows.get(0);
        assertThat(migration.getBoolean("applied_successful"), is(true));
        assertThat(migration.getInstant("executed_at"), is(not(nullValue())));
        assertThat(migration.getString("script_name"), is(CoreMatchers.equalTo("001_create_person_table.cql")));
        assertThat(migration.getString("script"), startsWith("CREATE TABLE"));
        migration = rows.get(1);
        assertThat(migration.getBoolean("applied_successful"), is(true));
        assertThat(migration.getInstant("executed_at"), is(not(nullValue())));
        assertThat(migration.getString("script_name"), is(CoreMatchers.equalTo("100_populate_person_table.cql")));
        assertThat(migration.getString("script"), startsWith("INSERT INTO"));
        session.close();
    }

    @Test
    public void shouldMigrateDatabaseWhenClusterGivenWithPrefixWithoutAutoKeyspaceCreation() {
        // CREATE KEYSPACE MANUALLY
        createKeyspace();

        // GIVEN
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        TestPropertyValues testValues = TestPropertyValues.of("cassandra.migration.keyspace-name:" + KEYSPACE)
                .and("cassandra.migration.table-prefix:test");
        testValues.applyTo(context);
        context.register(ClusterConfig.class, CassandraMigrationAutoConfiguration.class);
        context.refresh();

        // WHEN
        context.getBean(MigrationTask.class);

        // THEN
        CqlSession session = createSession();
        List<Row> rows = session.execute("SELECT * FROM " + KEYSPACE + ".test_schema_migration").all();
        assertThat(rows.size(), is(equalTo(1)));
        Row migration = rows.get(0);
        assertThat(migration.getBoolean("applied_successful"), is(true));
        assertThat(migration.getInstant("executed_at"), is(not(nullValue())));
        assertThat(migration.getString("script_name"), is(CoreMatchers.equalTo("001_create_person_table.cql")));
        assertThat(migration.getString("script"), startsWith("CREATE TABLE"));
        session.close();
    }

    @Test
    public void shouldMigrateDatabaseWhenClusterGivenWithKeyspaceSimpleReplicationAutoCreation() {
        // GIVEN
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        TestPropertyValues testValues = TestPropertyValues.of("cassandra.migration.keyspace-name:" + KEYSPACE);
        testValues.applyTo(context);
        context.register(ClusterConfig.class, CassandraMigrationAutoConfiguration.class);
        context.refresh();
        // WHEN
        context.getBean(MigrationTask.class);

        // THEN
        CqlSession session = createSession();
        KeyspaceMetadata keyspaceMetadata = session.getMetadata().getKeyspace(KEYSPACE).get();
        String replicationClass = keyspaceMetadata.getReplication().get("class");
        String replicationFactor = keyspaceMetadata.getReplication().get("replication_factor");
        assertThat(replicationClass, is("org.apache.cassandra.locator.SimpleStrategy"));
        assertThat(replicationFactor, is("1"));

        List<Row> rows = session.execute("SELECT * FROM " + KEYSPACE + ".schema_migration").all();
        assertThat(rows.size(), Matchers.is(equalTo(1)));
        Row migration = rows.get(0);
        assertThat(migration.getBoolean("applied_successful"), is(true));
        assertThat(migration.getInstant("executed_at"), is(not(nullValue())));
        assertThat(migration.getString("script_name"), is(CoreMatchers.equalTo("001_create_person_table.cql")));
        assertThat(migration.getString("script"), startsWith("CREATE TABLE"));
        session.close();
    }

    @Test
    public void shouldMigrateDatabaseWhenClusterGivenWithKeyspaceNetworkReplicationAutoCreation() {
        // GIVEN
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        TestPropertyValues testValues = TestPropertyValues.of("cassandra.migration.keyspace-name:" + KEYSPACE,
                "cassandra.migration.network-strategy.replications.datacenter1:1");
        testValues.applyTo(context);
        context.register(ClusterConfig.class, CassandraMigrationAutoConfiguration.class);
        context.refresh();
        // WHEN
        context.getBean(MigrationTask.class);

        // THEN
        CqlSession session = createSession();
        KeyspaceMetadata keyspaceMetadata = session.getMetadata().getKeyspace(KEYSPACE).get();
        String replicationClass = keyspaceMetadata.getReplication().get("class");
        String replicationOnDatacenter1 = keyspaceMetadata.getReplication().get("datacenter1");
        assertThat(replicationClass, is("org.apache.cassandra.locator.NetworkTopologyStrategy"));
        assertThat(replicationOnDatacenter1, is("1"));

        List<Row> rows = session.execute("SELECT * FROM " + KEYSPACE + ".schema_migration").all();
        assertThat(rows.size(), Matchers.is(equalTo(1)));
        Row migration = rows.get(0);
        assertThat(migration.getBoolean("applied_successful"), is(true));
        assertThat(migration.getInstant("executed_at"), is(not(nullValue())));
        assertThat(migration.getString("script_name"), is(CoreMatchers.equalTo("001_create_person_table.cql")));
        assertThat(migration.getString("script"), startsWith("CREATE TABLE"));
        session.close();
    }

    private static CqlSession createSession() {
        return new ClusterConfig().session();
    }

    @Configuration
    static class ClusterConfig {
        private static final String CASSANDRA_HOST = "127.0.0.1";
        private static final int REQUEST_TIMEOUT_IN_SECONDS = 30;

        @Bean(name = CQL_SESSION_BEAN_NAME)
        public CqlSession session() {
            DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(REQUEST_TIMEOUT_IN_SECONDS))
                    .withBoolean(DefaultDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, false)
                    .build();
            return new CqlSessionBuilder()
                    .withConfigLoader(loader)
                    .addContactPoint(new InetSocketAddress(CASSANDRA_HOST, 9042))
                    .withLocalDatacenter("datacenter1")
                    .build();
        }
    }
}