package org.cognitor.cassandra.migration.collector;

import java.util.*;

/**
 * This implementation of the {@link ScriptCollector} adds all collections
 * to a {@link TreeSet}. By doing this duplicate versions inside the script repository
 * are ignored and a first script wins strategy is implemented. Until version 1.0.2 this
 * was the default behavior. It means when there are two scripts that have the same version
 * but a different filename the first script that will be added will be the one executed for
 * this particular version.
 *
 * As this behavior is quite unpredictable the {@link FailOnDuplicatesCollector} is the new default
 * now.
 *
 * @author Patrick Kranz
 */
public class IgnoreDuplicatesCollector implements ScriptCollector {
    private final Set<ScriptFile> scriptFiles = new TreeSet<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void collect(ScriptFile scriptFile) {
        scriptFiles.add(scriptFile);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<ScriptFile> getScriptFiles() {
        return scriptFiles;
    }
}
