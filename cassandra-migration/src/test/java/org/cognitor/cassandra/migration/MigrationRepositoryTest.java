package org.cognitor.cassandra.migration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Patrick Kranz
 */
public class MigrationRepositoryTest {
    private MigrationRepository migrationRepository;

    @BeforeEach
    public void setUp() {
        this.migrationRepository = new MigrationRepository("cassandra/migrationtest/successful");
    }

    @Test
    public void shouldReturnThreeAsLatestVersionWhenThreeScriptsInResourcesGiven() {
        int version = migrationRepository.getLatestVersion();
        assertThat(version, is(equalTo(3)));
    }

    @Test
    public void shouldReturnOneScriptWhenRequestForAllScriptsSinceVersionTwoGiven() {
        List<DbMigration> scripts = migrationRepository.getMigrationsSinceVersion(2);
        assertThat(scripts.size(), is(equalTo(1)));
        assertThat(scripts.get(0).getVersion(), is(equalTo(3)));
    }

    @Test
    public void shouldReturnTwoScriptsWhenRequestForAllScriptsSinceVersionOneGiven() {
        List<DbMigration> scripts = migrationRepository.getMigrationsSinceVersion(1);
        assertThat(scripts.size(), is(equalTo(2)));
        assertThat(scripts.get(0).getVersion(), is(equalTo(2)));
        assertThat(scripts.get(1).getVersion(), is(equalTo(3)));
        assertThat(scripts.get(0).getMigrationScript().isEmpty(), is(false));
        assertThat(scripts.get(1).getMigrationScript().isEmpty(), is(false));
    }

    @Test
    public void shouldThrowExceptionWhenWrongScriptPathGiven() {
        assertThrows(MigrationException.class, () -> new MigrationRepository("cassandra"));
    }

    @Test
    public void shouldThrowExceptionWhenCqlScriptWithoutVersionGiven() {
        MigrationException exception = null;
        try {
            new MigrationRepository("cassandra/migrationtest/failing/wrongnaming");
        } catch (MigrationException e) {
            exception = e;
        }

        assertThat(exception, is(not(nullValue())));
        assertThat(exception.getScriptName(), is(equalTo("init.cql")));
        assertThat(exception.getStatement(), is(nullValue()));
        assertThat(exception.getMessage(), is(not(nullValue())));
    }

    @Test
    public void shouldReturnZeroAsVersionWhenEmptyDirectoryGiven() {
        MigrationRepository repository = new MigrationRepository("cassandra/migrationtest/empty");
        assertThat(repository.getLatestVersion(), is(equalTo(0)));
    }

    @Test
    public void shouldReturnCorrectVersionNumberWhenPathWithLeadingSlashGiven() {
        MigrationRepository repository = new MigrationRepository("/cassandra/migrationtest/empty");
        assertThat(repository.getLatestVersion(), is(equalTo(0)));
    }

    @Test
    public void shouldIgnoreComments() {
        List<DbMigration> scripts = migrationRepository.getMigrationsSinceVersion(1);
        assertThat(scripts.get(0).getMigrationScript().contains("--"), is(false));
        assertThat(scripts.get(0).getMigrationScript().contains("//"), is(false));
    }

}
