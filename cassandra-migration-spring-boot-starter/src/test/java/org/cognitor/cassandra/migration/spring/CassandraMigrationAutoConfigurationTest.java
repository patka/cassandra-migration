package org.cognitor.cassandra.migration.spring;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.cognitor.cassandra.migration.MigrationTask;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static org.cognitor.cassandra.migration.spring.CassandraMigrationAutoConfigurationTest.ClusterConfig.TEST_KEYSPACE;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

/**
 * @author Patrick Kranz
 */
public class CassandraMigrationAutoConfigurationTest {

    @Test
    public void shouldMigrateDatabaseWhenClusterGiven() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        addEnvironment(context, "cassandra.migration.keyspace-name:test_keyspace");
        context.register(ClusterConfig.class, CassandraMigrationAutoConfiguration.class);
        context.refresh();
        Cluster cluster = context.getBean(Cluster.class);
        context.getBean(MigrationTask.class);
        try(Session session = cluster.connect(TEST_KEYSPACE)) {
            List<Row> rows = session.execute("SELECT * FROM schema_migration").all();
            assertThat(rows.size(), is(equalTo(1)));
            Row migration = rows.get(0);
            assertThat(migration.getBool("applied_successful"), is(true));
            assertThat(migration.getTimestamp("executed_at"), is(not(nullValue())));
            assertThat(migration.getString("script_name"), is(CoreMatchers.equalTo("001_create_person_table.cql")));
            assertThat(migration.getString("script"), startsWith("CREATE TABLE"));
        }
    }

    @Test
    public void shouldMigrateDatabaseWhenClusterGivenWithPrefix() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        addEnvironment(context, "cassandra.migration.keyspace-name:test_keyspace");
        addEnvironment(context, "cassandra.migration.table-prefix:test_");
        context.register(ClusterConfig.class, CassandraMigrationAutoConfiguration.class);
        context.refresh();
        Cluster cluster = context.getBean(Cluster.class);
        context.getBean(MigrationTask.class);
        try (Session session = cluster.connect(TEST_KEYSPACE)) {
            List<Row> rows = session.execute("SELECT * FROM test_schema_migration").all();
            assertThat(rows.size(), is(equalTo(1)));
            Row migration = rows.get(0);
            assertThat(migration.getBool("applied_successful"), is(true));
            assertThat(migration.getTimestamp("executed_at"), is(not(nullValue())));
            assertThat(migration.getString("script_name"), is(CoreMatchers.equalTo("001_create_person_table.cql")));
            assertThat(migration.getString("script"), startsWith("CREATE TABLE"));
        }
    }

    @Configuration
    static class ClusterConfig {
        static final String TEST_KEYSPACE = "test_keyspace";

        private static final String CASSANDRA_INIT_SCRIPT = "cassandraTestInit.cql";
        private static final String LOCALHOST = "127.0.0.1";

        private static final String YML_FILE_LOCATION = "cassandra.yml";
        private ClassPathCQLDataSet dataSet;
        private Cluster cluster;

        @Bean
        public Cluster cluster() throws Exception {
            dataSet = new ClassPathCQLDataSet(CASSANDRA_INIT_SCRIPT, TEST_KEYSPACE);
            cluster = new Cluster.Builder().addContactPoints(LOCALHOST).withPort(9142).build();
            init();
            return cluster;
        }

        private void init() throws Exception {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra(YML_FILE_LOCATION, 30 * 1000L);
            loadTestData();
        }

        private void loadTestData() {
            Session session = cluster.connect();
            CQLDataLoader dataLoader = new CQLDataLoader(session);
            dataLoader.load(dataSet);
            session.close();
        }

    }
}