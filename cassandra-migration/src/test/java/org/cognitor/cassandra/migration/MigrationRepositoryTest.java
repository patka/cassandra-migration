package org.cognitor.cassandra.migration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Patrick Kranz
 */
public class MigrationRepositoryTest {
    private MigrationRepository migrationRepository;

    @Before
    public void setUp() {
        this.migrationRepository = new MigrationRepository("cassandra/migrationtest/successful");
    }

    @Test
    public void shouldReturnTwoAsLatestVersionWhenTwoScriptsInResourcesGiven() {
        int version = migrationRepository.getLatestVersion();
        assertThat(version, is(equalTo(2)));
    }

    @Test
    public void shouldReturnOneScriptWhenRequestForAllScriptsSinceVersionTwoGiven() {
        List<DbMigration> scripts = migrationRepository.getMigrationsSinceVersion(1);
        assertThat(scripts.size(), is(equalTo(1)));
        assertThat(scripts.get(0).getVersion(), is(equalTo(2)));
    }

    @Test
    public void shouldReturnTwoScriptsWhenRequestForAllScriptsSinceVersionOneGiven() {
        List<DbMigration> scripts = migrationRepository.getMigrationsSinceVersion(0);
        assertThat(scripts.size(), is(equalTo(2)));
        assertThat(scripts.get(0).getVersion(), is(equalTo(1)));
        assertThat(scripts.get(1).getVersion(), is(equalTo(2)));
        assertThat(scripts.get(0).getMigrationScript().isEmpty(), is(false));
        assertThat(scripts.get(1).getMigrationScript().isEmpty(), is(false));
    }

    @Test(expected = MigrationException.class)
    public void shouldThrowExceptionWhenWrongScriptPathGiven() {
        new MigrationRepository("cassandra");
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
