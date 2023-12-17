package org.cognitor.cassandra.migration.collector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

/**
 * @author Patrick Kranz
 */
public class IgnoreDuplicatesCollectorTest {
    private ScriptCollector scriptCollector;

    @BeforeEach
    public void before() {
        this.scriptCollector = new IgnoreDuplicatesCollector();
    }

    @Test
    public void shouldReturnAllScriptsWhenNoDuplicateScriptFileGiven() {
        scriptCollector.collect(new ScriptFile(0, "/0_init.cql", "init.cql"));
        scriptCollector.collect(new ScriptFile(1, "/1_add-table.cql", "add_table.cql"));

        assertThat(scriptCollector.getScriptFiles().size(), is(equalTo(2)));
    }

    @Test
    public void shouldReturnFirstScriptWhenDuplicateScriptFileVersionsGiven() {
        ScriptFile firstOne = new ScriptFile(0, "/0_init.cql", "init.cql");
        ScriptFile secondOne = new ScriptFile(0, "/0_another-init.cql", "another-init.cql");
        scriptCollector.collect(firstOne);
        scriptCollector.collect(secondOne);

        assertThat(scriptCollector.getScriptFiles().size(), is(equalTo(1)));
        assertThat(scriptCollector.getScriptFiles().iterator().next(), is(equalTo(firstOne)));
    }

    @Test
    public void shouldReturnEmptyCollectionWhenNoScriptFileGiven() {
        assertThat(scriptCollector.getScriptFiles(), is(not(nullValue())));
        assertThat(scriptCollector.getScriptFiles(), is(empty()));
    }
}