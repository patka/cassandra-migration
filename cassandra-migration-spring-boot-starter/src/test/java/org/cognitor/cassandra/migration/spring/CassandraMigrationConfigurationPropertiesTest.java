package org.cognitor.cassandra.migration.spring;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import org.cognitor.cassandra.migration.keyspace.NetworkStrategy;
import org.cognitor.cassandra.migration.keyspace.ReplicationStrategy;
import org.cognitor.cassandra.migration.keyspace.SimpleStrategy;
import org.junit.Test;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.NestedExceptionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

/**
 * @author Patrick Kranz
 */
public class CassandraMigrationConfigurationPropertiesTest {

    @Test
    public void shouldThrowIllegalStateExceptionWhenKeyspaceNameIsNotProvided() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(CassandraMigrationAutoConfiguration.class);

        try {
            context.refresh();
            fail("IllegalStateException was not thrown on missing keyspace-name property");
        } catch (Exception e) {
            Throwable rootCause = NestedExceptionUtils.getRootCause(e);
            assertThat(rootCause, is(notNullValue()));
            assertThat(rootCause, isA(IllegalStateException.class));
            assertThat(rootCause.getMessage(), is("Please specify ['cassandra.migration.keyspace-name'] in order to migrate your database"));
        }
    }

    @Test
    public void shouldPopulatePropertiesWhenPropertiesGivenWithNetworkStrategy() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        TestPropertyValues testValues = TestPropertyValues.of(
                "cassandra.migration.script-location:cassandra/migrationpath",
                "cassandra.migration.keyspace-name:test_keyspace",
                "cassandra.migration.network-strategy.replications.boston:1",
                "cassandra.migration.network-strategy.replications.tokyo:3",
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
        assertThat(properties.hasKeyspaceName(), is(true));
        assertThat(properties.getKeyspaceName(), is(equalTo("test_keyspace")));
        ReplicationStrategy replicationStrategy = properties.getReplicationStrategy();
        assertThat(replicationStrategy, is(instanceOf(NetworkStrategy.class)));
        assertThat(((NetworkStrategy)replicationStrategy).getDataCenters(), is(new LinkedHashMap<String, Integer>() {{
            put("boston", 1);
            put("tokyo", 3);
        }}));
        assertThat(properties.getScriptLocations(), is(equalTo(Collections.singletonList("cassandra/migrationpath"))));
        assertThat(properties.getStrategy(), is(equalTo(ScriptCollectorStrategy.IGNORE_DUPLICATES)));
        assertThat(properties.getConsistencyLevel(), is(equalTo(DefaultConsistencyLevel.ALL)));
        assertThat(properties.getTablePrefix(), is(equalTo("prefix")));
        assertThat(properties.isWithConsensus(), is(true));
        assertThat(properties.getExecutionProfileName(), is(equalTo("testProfile")));
    }

    @Test
    public void shouldPopulatePropertiesWhenPropertiesGivenWithSimpleStrategy() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        TestPropertyValues testValues = TestPropertyValues.of(
                "cassandra.migration.script-location:cassandra/migrationpath",
                "cassandra.migration.keyspace-name:test_keyspace",
                "cassandra.migration.strategy:IGNORE_DUPLICATES",
                "cassandra.migration.consistency-level:all",
                "cassandra.migration.table-prefix:prefix",
                "cassandra.migration.simple-strategy.replication-factor:2",
                "cassandra.migration.with-consensus:true",
                "cassandra.migration.execution-profile-name:testProfile");
        testValues.applyTo(context);
        context.register(CassandraMigrationAutoConfiguration.class);
        context.refresh();
        CassandraMigrationConfigurationProperties properties =
                context.getBean(CassandraMigrationConfigurationProperties.class);
        assertThat(properties.hasKeyspaceName(), is(true));
        assertThat(properties.getKeyspaceName(), is(equalTo("test_keyspace")));
        ReplicationStrategy replicationStrategy = properties.getReplicationStrategy();
        assertThat(replicationStrategy, is(instanceOf(SimpleStrategy.class)));
        assertThat(((SimpleStrategy)replicationStrategy).getReplicationFactor(), is(2));
        assertThat(properties.getScriptLocations(), is(equalTo(Collections.singletonList("cassandra/migrationpath"))));
        assertThat(properties.getStrategy(), is(equalTo(ScriptCollectorStrategy.IGNORE_DUPLICATES)));
        assertThat(properties.getConsistencyLevel(), is(equalTo(DefaultConsistencyLevel.ALL)));
        assertThat(properties.getTablePrefix(), is(equalTo("prefix")));
        assertThat(properties.isWithConsensus(), is(true));
        assertThat(properties.getExecutionProfileName(), is(equalTo("testProfile")));
    }

    @Test
    public void shouldPopulatePropertiesWhenPropertiesFileGivenMultipleLocations() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        TestPropertyValues testValues = TestPropertyValues.of(
                "cassandra.migration.script-locations:cassandra/migrationpath,cassandra/other",
                "cassandra.migration.keyspace-name:test_keyspace");
        testValues.applyTo(context);
        context.register(CassandraMigrationAutoConfiguration.class);
        context.refresh();
        CassandraMigrationConfigurationProperties properties =
                context.getBean(CassandraMigrationConfigurationProperties.class);
        assertThat(properties.hasKeyspaceName(), is(true));
        assertThat(properties.getScriptLocations(), is(equalTo(Arrays.asList("cassandra/migrationpath", "cassandra/other"))));
        assertThat(properties.getStrategy(), is(equalTo(ScriptCollectorStrategy.FAIL_ON_DUPLICATES)));
        assertThat(properties.getTablePrefix(), is(equalTo("")));
        assertThat(properties.isWithConsensus(), is(false));
        assertThat(properties.getExecutionProfileName(), is(nullValue()));
    }

    @Test
    public void shouldReturnDefaultValuesWhenNoOptionalPropertiesGiven() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        TestPropertyValues testValues = TestPropertyValues.of(
                "cassandra.migration.keyspace-name:test_keyspace");
        testValues.applyTo(context);
        context.register(CassandraMigrationAutoConfiguration.class);
        context.refresh();
        CassandraMigrationConfigurationProperties properties =
                context.getBean(CassandraMigrationConfigurationProperties.class);
        ReplicationStrategy replicationStrategy = properties.getReplicationStrategy();
        assertThat(replicationStrategy, is(instanceOf(SimpleStrategy.class)));
        assertThat(((SimpleStrategy)replicationStrategy).getReplicationFactor(), is(1));
        assertThat(properties.getScriptLocations(), is(equalTo(Collections.singletonList("cassandra/migration"))));
        assertThat(properties.getStrategy(), is(equalTo(ScriptCollectorStrategy.FAIL_ON_DUPLICATES)));
        assertThat(properties.getTablePrefix(), is(equalTo("")));
        assertThat(properties.isWithConsensus(), is(false));
        assertThat(properties.getExecutionProfileName(), is(nullValue()));
    }
}
