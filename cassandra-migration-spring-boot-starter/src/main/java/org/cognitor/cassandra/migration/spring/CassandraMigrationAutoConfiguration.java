package org.cognitor.cassandra.migration.spring;

import com.datastax.driver.core.Cluster;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;
import org.cognitor.cassandra.migration.collector.IgnoreDuplicatesCollector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Patrick Kranz
 */
@Configuration
@EnableConfigurationProperties(CassandraMigrationConfigurationProperties.class)
@AutoConfigureAfter(CassandraAutoConfiguration.class)
@ConditionalOnClass(Cluster.class)
public class CassandraMigrationAutoConfiguration {
    private final CassandraMigrationConfigurationProperties properties;

    @Autowired
    public CassandraMigrationAutoConfiguration(CassandraMigrationConfigurationProperties properties) {
        this.properties = properties;
    }


    @Bean(initMethod = "migrate")
    @ConditionalOnBean(Cluster.class)
    @ConditionalOnMissingBean(MigrationTask.class)
    public MigrationTask migrationTask(Cluster cluster) {
        if (!properties.hasKeyspaceName()) {
            throw new IllegalStateException("Please specify ['cassandra.migration.keyspace-name'] in" +
                    " order to migrate your database");
        }

        MigrationRepository migrationRepository = createRepository();
        return new MigrationTask(new Database(cluster, properties.getKeyspaceName()),
                migrationRepository);
    }

    private MigrationRepository createRepository() {
        if (properties.getStrategy() == ScriptCollectorStrategy.FAIL_ON_DUPLICATES) {
            return new MigrationRepository(properties.getScriptLocation());
        }
        return new MigrationRepository(properties.getScriptLocation(), new IgnoreDuplicatesCollector());
    }
}
