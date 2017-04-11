package org.cognitor.cassandra.migration.collector;

import java.util.Collection;

/**
 * Implementations of this interface can be used to modify the behavior
 * of the {@link org.cognitor.cassandra.migration.MigrationRepository} when it comes
 * to the scripts that are considered. While the repository is scanning for
 * migration scripts, every script that matches the filename pattern is passed
 * to an implementation of this interfaces collect method.
 *
 * The <code>collect</code> method can then decide how it will deal with
 * this file which usually comes down to ignoring the file or considering the file.
 *
 * At the end of the collector process the <code>getScriptFiles</code> method is called in
 * order to return all relevant script files.
 *
 * @author Patrick Kranz
 */
public interface ScriptCollector {
    /**
     * Called for every potential script that is found during the collector
     * process of the repository.
     *
     * @param scriptFile an object containing information about the script. Never null.
     */
    void collect(ScriptFile scriptFile);

    /**
     * Called when the collector process in the repository is done in order to return
     * all relevant scripts. There is no need to sort the files as this is done by the repository.
     *
     * @return a collection of all files that are considered to be part of the repository or an empty collection
     * if none of the script files was considered.
     */
    Collection<ScriptFile> getScriptFiles();
}
