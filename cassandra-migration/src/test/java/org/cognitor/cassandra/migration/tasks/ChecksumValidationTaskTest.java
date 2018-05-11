package org.cognitor.cassandra.migration.tasks;

import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.DbMigration;
import org.cognitor.cassandra.migration.MigrationException;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * @author Patrick Kranz
 */
@RunWith(MockitoJUnitRunner.class)
public class ChecksumValidationTaskTest {

    @Mock
    private Database databaseMock;

    @Mock
    private MigrationRepository repositoryMock;

    @Test
    public void shouldNotThrowExceptionWhenMatchingChecksumsGiven() {
        List<DbMigration> migrations = new ArrayList<>();
        migrations.add(new DbMigration("001_test.cql", 1, "some script"));
        migrations.add(new DbMigration("002_test_again.cql", 2, "more script"));
        when(databaseMock.loadMigrations()).thenReturn(migrations);
        when(repositoryMock.getMigrationsSinceVersion(0)).thenReturn(migrations);

        ChecksumValidationTask task = new ChecksumValidationTask(databaseMock, repositoryMock);
        task.execute();
    }

    @Test
    public void shouldThrowExceptionWhenSameScriptWithDifferentChecksumGiven() {
        List<DbMigration> dbMigrations = new ArrayList<>();
        List<DbMigration> repoMigrations = new ArrayList<>();
        dbMigrations.add(new DbMigration("001_test.cql", 1, "some script"));
        dbMigrations.add(new DbMigration("002_another_test.cql", 2, "another script"));
        repoMigrations.add(new DbMigration("001_test.cql", 1, "some script", 42L, new Date()));
        repoMigrations.add(new DbMigration("002_another_test.cql", 2, "another script", 21L, new Date()));
        when(databaseMock.loadMigrations()).thenReturn(dbMigrations);
        when(repositoryMock.getMigrationsSinceVersion(0)).thenReturn(repoMigrations);

        ChecksumValidationTask task = new ChecksumValidationTask(databaseMock, repositoryMock);
        try {
            task.execute();
        } catch (MigrationException exception) {
            assertThat(exception.getMessage().startsWith("Checksum validation failed."), is(true));
            return;
        }
        fail("Exception did not happen");
    }

    @Test
    public void shouldThrowExceptionWhenMoreMigrationsInsideDatabaseGiven() {
        List<DbMigration> dbMigrations = new ArrayList<>();
        List<DbMigration> repoMigrations = new ArrayList<>();
        dbMigrations.add(new DbMigration("001_test.cql", 1, "some script"));
        dbMigrations.add(new DbMigration("002_another_test.cql", 2, "another script"));
        repoMigrations.add(new DbMigration("001_test.cql", 1, "some script", 42L, new Date()));
        when(databaseMock.loadMigrations()).thenReturn(dbMigrations);
        when(repositoryMock.getMigrationsSinceVersion(0)).thenReturn(repoMigrations);

        ChecksumValidationTask task = new ChecksumValidationTask(databaseMock, repositoryMock);
        try {
            task.execute();
        } catch (MigrationException exception) {
            assertThat(exception.getMessage(), is(equalTo("Error during validation of checksums: There are 2 migrations in the database but only 1 are coming with the application.")));
            return;
        }
        fail("Exception did not happen");
    }
}