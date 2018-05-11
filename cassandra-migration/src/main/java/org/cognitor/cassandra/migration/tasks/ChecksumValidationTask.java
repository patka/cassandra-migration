package org.cognitor.cassandra.migration.tasks;

import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.DbMigration;
import org.cognitor.cassandra.migration.MigrationException;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static org.cognitor.cassandra.migration.util.Ensure.notNull;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * This task validates if the checksums that are stored inside the database
 * are consistent with the ones calculated based on the scripts inside the
 * application. If a difference is found, the task will throw an exception
 * and report the script. <br/>
 * The task will perform the check for all the scripts at once and report
 * all failing scripts in order to avoid the well known "run fix run fix again"
 * scenario. <br/>
 * Currently the task is rather simple. If the number of scripts in the database
 * is higher than in the application, it will abort with an error. It assumes
 * that there is no script deletion inside the application.
 *
 * @author Patrick Kranz
 */
public class ChecksumValidationTask implements Task {
    private static final Logger LOGGER = getLogger(ChecksumValidationTask.class);
    private static final String ERRORS_MSG_DELIMITER = "\n\t* ";

    private final Database database;
    private final MigrationRepository repository;

    /**
     * Creates a migration task that uses the given database, repository and validation flag.
     *
     * @param database   the database that should be migrated
     * @param repository the repository that contains the migration scripts
     */
    public ChecksumValidationTask(Database database, MigrationRepository repository) {
        this.database = notNull(database, "database");
        this.repository = notNull(repository, "repository");
    }

    @Override
    public void execute() {
        List<DbMigration> persistentMigrations = database.loadMigrations();
        List<DbMigration> repositoryMigrations = repository.getMigrationsSinceVersion(0);
        LOGGER.info("About to validate the checksums of %s migrations", repositoryMigrations.size());

        ValidationResult result = new ValidationResult();
        if (repositoryMigrations.size() < persistentMigrations.size()) {
            throw new MigrationException(format(
                    "Error during validation of checksums: There are %d migrations in the database but only %d are coming with the application.",
                    persistentMigrations.size(),
                    repositoryMigrations.size()));
        }
        for (int i = 0; i < persistentMigrations.size(); i++) {
            DbMigration persistentMigration = persistentMigrations.get(i);
            DbMigration repoMigration = repositoryMigrations.get(i);
            if (persistentMigration.getChecksum() != repoMigration.getChecksum()) {
                result.addError(
                        format("Found different checksum for script '%s'. File was '%s' but in database is '%s'",
                                persistentMigration.getScriptName(),
                                repoMigration.getChecksum(),
                                persistentMigration.getChecksum()));
            }
        }
        if (result.hasErrors()) {
            throw new MigrationException(format("Checksum validation failed. Following errors haven been found: %s%s",
                    ERRORS_MSG_DELIMITER,
                    String.join(ERRORS_MSG_DELIMITER, result.getErrors())));
        }
        LOGGER.info("Validation of checksums finished. Everything is fine.");
    }

    private class ValidationResult {
        private List<String> errors = new ArrayList<>();

        void addError(String message) {
            errors.add(message);
        }

        List<String> getErrors() {
            return unmodifiableList(errors);
        }

        boolean hasErrors() {
            return !errors.isEmpty();
        }
    }
}
