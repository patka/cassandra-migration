package org.cognitor.cassandra.migration.spring;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import org.junit.Test;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.LinkedHashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

/**
 * @author Patrick Kranz
 */
public class CassandraMigrationConfigurationPropertiesTest {

    @Test
    public void shouldPopulatePropertiesWhenPropertiesGivenDeprecatedKeyspace() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        TestPropertyValues testValues = TestPropertyValues.of(
                "cassandra.migration.script-location:cassandra/migrationpath",
                "cassandra.migration.keyspace-name:test_keyspace",
                "cassandra.migration.strategy:IGNORE_DUPLICATES",
                "cassandra.migration.consistency-level:all",
                "cassandra.migration.table-prefix:prefix",
                "cassandra.migration.with-consensus:true",
                "cassandra.migration.execution-profile-name:testProfile");
        testValues.applyTo(context);
        context.register(CassandraMigrationAutoConfiguration.class);
        context.refresh();
        CassandraMigrationConfigurationProperties properties =
                context.getBean(CassandraMigrationConfigurationProperties.class);
        assertThat(properties.getKeyspace().getKeyspaceName(), is(equalTo("test_keyspace")));
        assertThat(properties.getKeyspace().getReplicationStrategy(), is(KeyspaceReplicationStrategy.SIMPLE));
        assertThat(properties.getScriptLocation(), is(equalTo("cassandra/migrationpath")));
        assertThat(properties.getStrategy(), is(equalTo(ScriptCollectorStrategy.IGNORE_DUPLICATES)));
        assertThat(properties.getConsistencyLevel(), is(equalTo(DefaultConsistencyLevel.ALL)));
        assertThat(properties.getTablePrefix(), is(equalTo("prefix")));
        assertThat(properties.isWithConsensus(), is(true));
        assertThat(properties.getExecutionProfileName(), is(equalTo("testProfile")));
    }

    @Test
    public void shouldPopulatePropertiesWhenPropertiesGiven() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        TestPropertyValues testValues = TestPropertyValues.of(
                "cassandra.migration.script-location:cassandra/migrationpath",
                "cassandra.migration.keyspace.keyspace-name:test_keyspace",
                "cassandra.migration.keyspace.replication-strategy:NETWORK",
                "cassandra.migration.keyspace.replications.boston:1",
                "cassandra.migration.keyspace.replications.tokyo:3",
                "cassandra.migration.strategy:IGNORE_DUPLICATES",
                "cassandra.migration.consistency-level:all",
                "cassandra.migration.table-prefix:prefix",
                "cassandra.migration.with-consensus:true",
                "cassandra.migration.execution-profile-name:testProfile");
        testValues.applyTo(context);
        context.register(CassandraMigrationAutoConfiguration.class);
        context.refresh();
        CassandraMigrationConfigurationProperties properties =
                context.getBean(CassandraMigrationConfigurationProperties.class);
        assertThat(properties.getKeyspace().getKeyspaceName(), is(equalTo("test_keyspace")));
        assertThat(properties.getKeyspace().getReplicationStrategy(), is(KeyspaceReplicationStrategy.NETWORK));
        assertThat(properties.getKeyspace().getReplications(), is(new LinkedHashMap<String, Integer>() {{
            put("boston", 1);
            put("tokyo", 3);
        }}));
        assertThat(properties.getScriptLocation(), is(equalTo("cassandra/migrationpath")));
        assertThat(properties.getStrategy(), is(equalTo(ScriptCollectorStrategy.IGNORE_DUPLICATES)));
        assertThat(properties.getConsistencyLevel(), is(equalTo(DefaultConsistencyLevel.ALL)));
        assertThat(properties.getTablePrefix(), is(equalTo("prefix")));
        assertThat(properties.isWithConsensus(), is(true));
        assertThat(properties.getExecutionProfileName(), is(equalTo("testProfile")));
    }

    @Test
    public void shouldReturnDefaultValuesWhenNoOptionalPropertiesGiven() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        context.register(CassandraMigrationAutoConfiguration.class);
        context.refresh();
        CassandraMigrationConfigurationProperties properties =
                context.getBean(CassandraMigrationConfigurationProperties.class);
        assertThat(properties.getKeyspace().hasKeyspaceName(), is(false));
        assertThat(properties.getKeyspace().getReplicationStrategy(), is(KeyspaceReplicationStrategy.SIMPLE));
        assertThat(properties.getScriptLocation(), is(equalTo("cassandra/migration")));
        assertThat(properties.getStrategy(), is(equalTo(ScriptCollectorStrategy.FAIL_ON_DUPLICATES)));
        assertThat(properties.getTablePrefix(), is(equalTo("")));
        assertThat(properties.isWithConsensus(), is(false));
        assertThat(properties.getExecutionProfileName(), is(nullValue()));
    }
}
