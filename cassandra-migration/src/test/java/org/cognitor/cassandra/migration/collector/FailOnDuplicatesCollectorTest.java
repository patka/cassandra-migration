package org.cognitor.cassandra.migration.collector;

import org.cognitor.cassandra.migration.MigrationException;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Patrick Kranz
 */
public class FailOnDuplicatesCollectorTest {
    private ScriptCollector scriptCollector;

    @Before
    public void before() {
        this.scriptCollector = new FailOnDuplicatesCollector();
    }

    @Test
    public void shouldReturnAllScriptsWhenNoDuplicateScriptFileGiven() {
        scriptCollector.collect(new ScriptFile(0, "/0_init.cql", "init.cql"));
        scriptCollector.collect(new ScriptFile(1, "/1_add-table.cql", "add_table.cql"));

        assertThat(scriptCollector.getScriptFiles().size(), is(equalTo(2)));
    }

    @Test(expected = MigrationException.class)
    public void shouldThrowExceptionWhenDuplicateScriptFileVersionsGiven() {
        scriptCollector.collect(new ScriptFile(0, "/0_init.cql", "init.cql"));
        scriptCollector.collect(new ScriptFile(0, "/0_another-init.cql", "another-init.cql"));
    }

    @Test
    public void shouldReturnEmptyCollectionWhenNoScriptFileGiven() {
        assertThat(scriptCollector.getScriptFiles(), is(not(nullValue())));
        assertThat(scriptCollector.getScriptFiles(), is(empty()));
    }
}