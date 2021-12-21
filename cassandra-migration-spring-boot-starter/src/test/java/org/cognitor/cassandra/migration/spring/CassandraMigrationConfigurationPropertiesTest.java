package org.cognitor.cassandra.migration.spring;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import org.junit.Test;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Patrick Kranz
 */
public class CassandraMigrationConfigurationPropertiesTest {

    @Test
    public void shouldPopulatePropertiesWhenPropertiesFileGiven() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        TestPropertyValues testValues = TestPropertyValues.of(
                "cassandra.migration.script-location:cassandra/migrationpath",
                "cassandra.migration.keyspace-name:test_keyspace",
                "cassandra.migration.strategy:IGNORE_DUPLICATES",
                "cassandra.migration.consistency-level:all",
                "cassandra.migration.table-prefix:prefix",
                "cassandra.migration.with-consensus:true");
        testValues.applyTo(context);
        context.register(CassandraMigrationAutoConfiguration.class);
        context.refresh();
        CassandraMigrationConfigurationProperties properties =
                context.getBean(CassandraMigrationConfigurationProperties.class);
        assertThat(properties.getKeyspaceName(), is(equalTo("test_keyspace")));
        assertThat(properties.getScriptLocation(), is(equalTo("cassandra/migrationpath")));
        assertThat(properties.getStrategy(), is(equalTo(ScriptCollectorStrategy.IGNORE_DUPLICATES)));
        assertThat(properties.getConsistencyLevel(), is(equalTo(DefaultConsistencyLevel.ALL)));
        assertThat(properties.getTablePrefix(), is(equalTo("prefix")));
        assertThat(properties.isWithConsensus(), is(true));
    }

    @Test
    public void shouldReturnDefaultValuesWhenNoOptionalPropertiesGiven() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        context.register(CassandraMigrationAutoConfiguration.class);
        context.refresh();
        CassandraMigrationConfigurationProperties properties =
                context.getBean(CassandraMigrationConfigurationProperties.class);
        assertThat(properties.hasKeyspaceName(), is(false));
        assertThat(properties.getScriptLocation(), is(equalTo("cassandra/migration")));
        assertThat(properties.getStrategy(), is(equalTo(ScriptCollectorStrategy.FAIL_ON_DUPLICATES)));
        assertThat(properties.getTablePrefix(), is(equalTo("")));
        assertThat(properties.isWithConsensus(), is(false));
    }
}