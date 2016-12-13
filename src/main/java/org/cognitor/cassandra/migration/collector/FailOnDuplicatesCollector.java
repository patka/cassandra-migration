package org.cognitor.cassandra.migration.collector;

import org.cognitor.cassandra.migration.MigrationException;

import java.util.*;

import static java.lang.String.format;

/**
 * This strategy throws a {@link MigrationException} when there are two different
 * migration scripts with the same version inside the repository. It will fail immediately
 * and the collector process is interrupted.
 *
 * This is the default script collection strategy as it is a common problem in
 * projects with many developers or branches.
 *
 * @author Patrick Kranz
 */
public class FailOnDuplicatesCollector implements ScriptCollector {
    private final List<ScriptFile> scripts = new ArrayList<>();
    private final Set<Integer> versions = new HashSet<>();

    /**
     * {@inheritDoc}
     *
     * Checks for every <code>scriptFile</code> that was added if there was
     * already a script with the same version added. Throws an exception of
     * it is the case.
     *
     * @param scriptFile an object containing information about the script. Never null.
     * @throws MigrationException if a two different scripts with the same version are added
     */
    @Override
    public void collect(ScriptFile scriptFile) {
        if (!versions.contains(scriptFile.getVersion())) {
            scripts.add(scriptFile);
            versions.add(scriptFile.getVersion());
        } else {
            throw new MigrationException(
                    format("Found two different files for version %d", scriptFile.getVersion()),
                    scriptFile.getScriptName());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<ScriptFile> getScriptFiles() {
        return scripts;
    }
}
