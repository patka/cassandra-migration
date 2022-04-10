package org.cognitor.cassandra.migration.spring;

import com.datastax.oss.driver.api.core.CqlSession;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationConfiguration;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;
import org.cognitor.cassandra.migration.collector.FailOnDuplicatesCollector;
import org.cognitor.cassandra.migration.collector.IgnoreDuplicatesCollector;
import org.cognitor.cassandra.migration.keyspace.Keyspace;
import org.cognitor.cassandra.migration.keyspace.ReplicationStrategy;
import org.cognitor.cassandra.migration.scanner.ScannerRegistry;
import org.cognitor.cassandra.migration.spring.scanner.SpringBootLocationScanner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
@ConditionalOnClass(CqlSession.class)
public class CassandraMigrationAutoConfiguration {
    public static final String CQL_SESSION_BEAN_NAME = "cassandraMigrationCqlSession";
    public static final String MIGRATION_TASK_BEAN_NAME = "migrationTask";
    private final CassandraMigrationConfigurationProperties properties;

    @Autowired
    public CassandraMigrationAutoConfiguration(CassandraMigrationConfigurationProperties properties) {
        this.properties = properties;
    }

    @Bean(name = MIGRATION_TASK_BEAN_NAME, initMethod = "migrate")
    @ConditionalOnBean(value = CqlSession.class)
    @ConditionalOnMissingBean(MigrationTask.class)
    public MigrationTask migrationTask(@Qualifier(CQL_SESSION_BEAN_NAME) CqlSession cqlSession) {
        if (!properties.hasKeyspaceName()) {
            throw new IllegalStateException("Please specify ['cassandra.migration.keyspace.keyspace-name'] in" +
                    " order to migrate your database");
        }

        MigrationRepository migrationRepository = createRepository();
        MigrationConfiguration configuration = createConfiguration();
        return new MigrationTask(new Database(cqlSession, configuration)
                .setConsistencyLevel(properties.getConsistencyLevel()),
                migrationRepository,
                properties.isWithConsensus());
    }

    private MigrationConfiguration createConfiguration() {
        String keyspaceName = properties.getKeyspaceName();
        ReplicationStrategy replicationStrategy = properties.getReplicationStrategy();

        return new MigrationConfiguration()
                .withKeyspace(new Keyspace(keyspaceName).with(replicationStrategy))
                .withTablePrefix(properties.getTablePrefix())
                .withExecutionProfile(properties.getExecutionProfileName());
    }

    private MigrationRepository createRepository() {
        ScannerRegistry registry = new ScannerRegistry();
        registry.register(ScannerRegistry.JAR_SCHEME, new SpringBootLocationScanner());
        if (properties.getStrategy() == ScriptCollectorStrategy.FAIL_ON_DUPLICATES) {
            return new MigrationRepository(properties.getScriptLocation(), new FailOnDuplicatesCollector(), registry);
        }
        return new MigrationRepository(properties.getScriptLocation(), new IgnoreDuplicatesCollector(), registry);
    }
}
