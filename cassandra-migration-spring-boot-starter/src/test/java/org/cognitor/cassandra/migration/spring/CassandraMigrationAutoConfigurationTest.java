package org.cognitor.cassandra.migration.spring;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.Row;
import org.cognitor.cassandra.migration.MigrationTask;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

/**
 * @author Patrick Kranz
 */
public class CassandraMigrationAutoConfigurationTest {
    private static final String KEYSPACE = "test_keyspace";

    @Before
    public void before() {
        CqlSession session = createSession();
        session.execute("CREATE KEYSPACE test_keyspace WITH REPLICATION = " +
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
    public void shouldMigrateDatabaseWhenClusterGiven() {
        // GIVEN
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        addEnvironment(context, "cassandra.migration.keyspace-name:" + KEYSPACE);
        context.register(ClusterConfig.class, CassandraMigrationAutoConfiguration.class);
        context.refresh();
        // WHEN
        context.getBean(MigrationTask.class);

        // THEN
        CqlSession session = createSession();
        List<Row> rows = session.execute("SELECT * FROM " + KEYSPACE + ".schema_migration").all();
        assertThat(rows.size(), Matchers.is(equalTo(1)));
        Row migration = rows.get(0);
        assertThat(migration.getBoolean("applied_successful"), is(true));
        assertThat(migration.getInstant("executed_at"), is(not(nullValue())));
        assertThat(migration.getString("script_name"), is(CoreMatchers.equalTo("001_create_person_table.cql")));
        assertThat(migration.getString("script"), startsWith("CREATE TABLE"));
    }

    @Test
    public void shouldMigrateDatabaseWhenClusterGivenWithPrefix() {
        // GIVEN
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        addEnvironment(context, "cassandra.migration.keyspace-name:" + KEYSPACE);
        addEnvironment(context, "cassandra.migration.table-prefix:test");
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