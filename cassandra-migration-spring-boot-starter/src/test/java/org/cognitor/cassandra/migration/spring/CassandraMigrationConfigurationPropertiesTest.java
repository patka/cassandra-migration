package org.cognitor.cassandra.migration.spring;

import com.datastax.driver.core.ConsistencyLevel;
import org.junit.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

/**
 * @author Patrick Kranz
 */
public class CassandraMigrationConfigurationPropertiesTest {

    @Test
    public void shouldPopulatePropertiesWhenPropertiesFileGiven() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        addEnvironment(context, "cassandra.migration.script-location:cassandra/migrationpath");
        addEnvironment(context, "cassandra.migration.keyspace-name:test_keyspace");
        addEnvironment(context, "cassandra.migration.strategy:IGNORE_DUPLICATES");
        addEnvironment(context, "cassandra.migration.consistency-level:ALL");
        addEnvironment(context, "cassandra.migration.recalculateChecksum:true");
        addEnvironment(context, "cassandra.migration.recalculateChecksumOnly:true");
        addEnvironment(context, "cassandra.migration.checksumValidation:true");
        addEnvironment(context, "cassandra.migration.checksumValidationOnly:true");
        addEnvironment(context, "cassandra.migration.checksumValidation:true");
        addEnvironment(context, "cassandra.migration.network-strategy.data-centers.boston=3");
        addEnvironment(context, "cassandra.migration.network-strategy.data-centers.seattle=2");
        addEnvironment(context, "cassandra.migration.network-strategy.data-centers.tokyo=2");
        context.register(PropertiesTestConfiguration.class);
        context.refresh();
        CassandraMigrationConfigurationProperties properties =
                context.getBean(CassandraMigrationConfigurationProperties.class);
        assertThat(properties.getKeyspaceName(), is(equalTo("test_keyspace")));
        assertThat(properties.getScriptLocation(), is(equalTo("cassandra/migrationpath")));
        assertThat(properties.getStrategy(), is(equalTo(ScriptCollectorStrategy.IGNORE_DUPLICATES)));
        assertThat(properties.getConsistencyLevel(), is(equalTo(ConsistencyLevel.ALL)));
        assertThat(properties.isChecksumValidation(), is(true));
        assertThat(properties.isChecksumValidationOnly(), is(true));
        assertThat(properties.isRecalculateChecksum(), is(true));
        assertThat(properties.isRecalculateChecksumOnly(), is(true));
        assertThat(properties.getReplicationStrategy().getName(), is("NetworkTopologyStrategy"));
    }

    @Test
    public void shouldReturnDefaultValuesWhenNoOptionalPropertiesGiven() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        context.register(PropertiesTestConfiguration.class);
        context.refresh();
        CassandraMigrationConfigurationProperties properties =
                context.getBean(CassandraMigrationConfigurationProperties.class);
        assertThat(properties.hasKeyspaceName(), is(false));
        assertThat(properties.getScriptLocation(), is(equalTo("cassandra/migration")));
        assertThat(properties.getStrategy(), is(equalTo(ScriptCollectorStrategy.FAIL_ON_DUPLICATES)));
    }

    @Configuration
    @EnableConfigurationProperties({CassandraMigrationConfigurationProperties.class})
    static class PropertiesTestConfiguration {
        private final CassandraMigrationConfigurationProperties properties;

        public PropertiesTestConfiguration(CassandraMigrationConfigurationProperties properties) {
            this.properties = properties;
        }
    }

}
