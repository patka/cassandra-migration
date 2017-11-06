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
    private final boolean checksumValidation;

    /**
     * Creates a migration task that uses the given database and repository.
     * Set checksum validation to false.
     *
     * @param database   the database that should be migrated
     * @param repository the repository that contains the migration scripts
     */
    public MigrationTask(Database database, MigrationRepository repository) {
        this(database, repository, false);
    }
    
    /**
     * Creates a migration task that uses the given database, repository and validation flag.
     *
     * @param database   the database that should be migrated
     * @param repository the repository that contains the migration scripts
     * @param checksumValidation flag that indicate if to validate checksums
     */
    public MigrationTask(Database database, MigrationRepository repository, boolean checksumValidation) {
        this.database = notNull(database, "database");
        this.repository = notNull(repository, "repository");
        this.checksumValidation= checksumValidation;
    }
    
    /**
     * Start the actual migration. Take the version of the database, get all required migrations and execute them or do
     * nothing if the DB is already up to date.
     * If checksumValidation flag is true, get all existing migration and compare their checksum.
     *
     * At the end the underlying database instance is closed.
     *
     * @return The number of migrated scripts. for any error, -1 is returned.
     * @throws MigrationException if a migration fails
     */
    public int migrate() {
        List<DbMigration> migrations = repository.getMigrationsSinceVersion(getRequestedVersion());
        
        int migratedScripts= 0;
        int databaseVersion = database.getVersion();
        for (DbMigration dbMigration : migrations) {
            if (dbMigration.getVersion() <= databaseVersion) {
            	// check validation
                String errorMessage = database.validateChecksum(dbMigration);
                if (errorMessage != null) {
                    LOGGER.error(String.format("Script %d validation failed: ", dbMigration.getVersion(), errorMessage));
                    return -1;
                }
            } else {
                // migrate to schema
                database.execute(dbMigration);
                migratedScripts++;
            }
        }
        
        if (this.checksumValidation) {
        	LOGGER.info(format("Keyspace %s validation ended successfully", database.getKeyspaceName(), database.getVersion()));
        }
        
        LOGGER.info(format("Migrated keyspace %s to version %d", database.getKeyspaceName(), database.getVersion()));
        database.close();
        return migratedScripts;
    }
    
    private int getRequestedVersion() {
        return this.checksumValidation ? 0 : database.getVersion();
    }
}
