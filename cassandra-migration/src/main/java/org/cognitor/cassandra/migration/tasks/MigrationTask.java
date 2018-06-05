package org.cognitor.cassandra.migration.tasks;

import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.DbMigration;
import org.cognitor.cassandra.migration.MigrationException;
import org.cognitor.cassandra.migration.MigrationRepository;
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
public class MigrationTask implements Task {
    private static final Logger LOGGER = getLogger(MigrationTask.class);

    private final Database database;
    private final MigrationRepository repository;

    /**
     * Creates a migration task that uses the given database, repository and validation flag.
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
     * If checksumValidation flag is true, get all existing migration and compare their checksum.
     * <p>
     * At the end the underlying database instance is closed.
     *
     * @throws MigrationException if a migration fails
     */
    @Override
    public void execute() {
        if (databaseIsUpToDate()) {
            LOGGER.info(format("Keyspace %s is already up to date at version %d", database.getKeyspaceName(),
                    database.getVersion()));
            return;
        }

        List<DbMigration> migrations = repository.getMigrationsSinceVersion(database.getVersion() + 1);
        migrations.forEach(database::execute);
        LOGGER.info(format("Migrated keyspace %s to version %d", database.getKeyspaceName(), database.getVersion()));
    }

    private boolean databaseIsUpToDate() {
        return database.getVersion() >= repository.getLatestVersion();
    }
}
