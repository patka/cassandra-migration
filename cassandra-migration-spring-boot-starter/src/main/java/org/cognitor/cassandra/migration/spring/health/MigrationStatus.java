package org.cognitor.cassandra.migration.spring.health;

import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.spring.CassandraMigrationConfigurationProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@EnableConfigurationProperties(CassandraMigrationConfigurationProperties.class)
public class MigrationStatus implements HealthIndicator {

    private final MigrationRepository migrationRepository;
    private final Database migrationDatabase;

    public MigrationStatus(MigrationRepository migrationRepository, Database database) {
        this.migrationRepository = migrationRepository;
        this.migrationDatabase = database;
    }

    @Override
    public Health health() {
        try {
            int latestVersion = migrationRepository.getLatestVersion();
            final int dbVersion = migrationDatabase.getVersion();

            if(latestVersion != dbVersion) {
                return Health.unknown()
                        .withDetail("databaseVersion", dbVersion)
                        .withDetail("sourceVersion", latestVersion)
                        .build();
            } else {
                return Health.up()
                        .withDetail("databaseVersion", dbVersion)
                        .withDetail("sourceVersion", latestVersion)
                        .build();
            }
        } catch (Throwable ex) {
          return Health.unknown().withException(ex).build();
        }
    }
}
