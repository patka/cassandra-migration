package org.cognitor.cassandra.migration.tasks;

import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.DbMigration;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.slf4j.Logger;

import java.util.List;

import static org.cognitor.cassandra.migration.util.Ensure.notNull;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * <p>
 * This task will recalculate the checksum of the migrations in the repository
 * and update the checksums in the database. While doing this it will also
 * update the script and file name information. This task will <b>NOT</b> perform a
 * migration. You should never change an existing migration once it was executed
 * except with a new migration that will increase the version number.
 * </p>
 * <p>
 * This task is supposed to be used in situations where a new comment in the
 * script file, fixing a typo or some other trivial change will invalidate the
 * checksum in the database and therefore make the checksum validation fail.
 * </p>
 *
 * @author Patrick Kranz
 */
public class RecalculateChecksumTask implements Task {
    private static final Logger LOGGER = getLogger(MigrationTask.class);

    private final Database database;
    private final MigrationRepository repository;
    private int startVersion = 0;
    private int endVersion = Integer.MAX_VALUE;

    /**
     * Creates a task that uses the given database, repository.
     *
     * @param database   the database that should was migrated with the scripts in the repository
     * @param repository the repository that contains the migration scripts
     */
    public RecalculateChecksumTask(Database database, MigrationRepository repository) {
        this.database = notNull(database, "database");
        this.repository = notNull(repository, "repository");
    }

    @Override
    public void execute() {
        List<DbMigration> migrations = this.repository.getMigrationsSinceVersion(startVersion, endVersion);
        migrations.forEach(database::updateMigration);
    }

    /**
     * Sets the first migration script version that should be updated. All subsequent
     * scripts until and not including endVersion will be updated. Default is 0
     * which means all scripts starting at the beginning will be updated.
     *
     * @param startVersion the first script of a range of scripts to be updated
     * @return the current instance of this class
     */
    public RecalculateChecksumTask setStartVersion(int startVersion) {
        this.startVersion = startVersion;
        return this;
    }

    /**
     * Sets the last migration script version that should be updated. All scripts with
     * a version smaller than this value and bigger than <code>startValue</code> will be updated.
     * Default is <code>Integer.MAX_VALUE</code> which means all scripts will be updated
     * that have a larger version than <code>startValue</code>.
     *
     * @param endVersion the end of a range of scripts to be updated. The last version is not included.
     * @return the current instance of this class
     */
    public RecalculateChecksumTask setEndVersion(int endVersion) {
        this.endVersion = endVersion;
        return this;
    }
}
