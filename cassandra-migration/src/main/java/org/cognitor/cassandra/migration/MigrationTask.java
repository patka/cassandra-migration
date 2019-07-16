package org.cognitor.cassandra.migration;

import org.slf4j.Logger;

import java.util.List;

import static java.lang.String.format;
import static org.cognitor.cassandra.migration.util.Ensure.notNull;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * The migration task is managing the database migrations. It checks which
 * schema version is in the database and retrieves the migrations that
 * need to be applied from the repository. Those migrations are than
 * executed against the database.
 *
 * @author Patrick Kranz
 */
public class MigrationTask {
    private static final Logger LOGGER = getLogger(MigrationTask.class);

    private final Database database;
    private final MigrationRepository repository;

    /**
     * Creates a migration task that uses the given database and repository.
     *
     * @param database   the database that should be migrated
     * @param repository the repository that contains the migration scripts
     */
    public MigrationTask(Database database, MigrationRepository repository) {
        this.database = notNull(database, "database");
        this.repository = notNull(repository, "repository");
    }

    /**
     * Start the actual migration. Take the version of the database, get all required migrations and execute them or do
     * nothing if the DB is already up to date.
     *
     * At the end the underlying database instance is closed.
     *
     * @throws MigrationException if a migration fails
     */
    public void migrate() {
        if (databaseIsUpToDate()) {
            LOGGER.info(format("Keyspace %s is already up to date at version %d", database.getKeyspaceName(),
                    database.getVersion()));
            database.close();
            return;
        }

        List<DbMigration> migrations = repository.getMigrationsSinceVersion(database.getVersion());
        migrations.forEach(database::execute);
        LOGGER.info(format("Migrated keyspace %s to version %d", database.getKeyspaceName(), database.getVersion()));
        database.close();
    }

    private boolean databaseIsUpToDate() {
        return database.getVersion() >= repository.getLatestVersion();
    }
}
