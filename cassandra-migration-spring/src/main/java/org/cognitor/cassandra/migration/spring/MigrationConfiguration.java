package org.cognitor.cassandra.migration.spring;

import com.datastax.driver.core.Cluster;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;
import org.cognitor.cassandra.migration.collector.IgnoreDuplicatesCollector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Patrick Kranz
 */
@Configuration
public class MigrationConfiguration {
    private MigrationConfigurationProperties properties;
    private Cluster cluster;

    @Autowired
    public MigrationConfiguration(MigrationConfigurationProperties properties, Cluster cluster) {
        this.properties = properties;
        this.cluster = cluster;
    }

    @Bean(initMethod = "migrate")
    public MigrationTask migrationTask() {
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
