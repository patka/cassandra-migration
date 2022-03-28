package org.cognitor.cassandra.migration.spring;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.Row;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import org.cognitor.cassandra.migration.MigrationTask;
import org.hamcrest.CoreMatchers;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraMigrationAutoConfigurationTest.class);

    @BeforeClass
    public static void initDb() {
        cassandra = new CassandraBuilder()
                .version("3.11.12")
                .build();
        cassandra.start();
        LOGGER.warn("cassandra started successfully");
    }

    @AfterClass
    public static void stopDb() {
        if (null != cassandra) {
            cassandra.stop();
        }
        LOGGER.warn("cassandra stopped successfully");
    }

    @Before
    public void before() {
        CqlSession session = createSession();
        session.execute("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION = " +
                "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.close();
    }

    @After
    public void after() {
        CqlSession session = createSession();
        session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE + ";");
        session.close();
    }

    @Test
    public void shouldMigrateDatabaseWhenClusterGivenWithMultipleLocations() {
        // GIVEN
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        TestPropertyValues testValues = TestPropertyValues.of("cassandra.migration.keyspace-name:" + KEYSPACE,
                "cassandra.migration.script-locations:cassandra/common,cassandra/dev");
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
    public void shouldMigrateDatabaseWhenClusterGivenWithPrefix() {
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

    private CqlSession createSession() {
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
