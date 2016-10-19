package org.cognitor.cassandra.migration.collector;

import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;

/**
 * This file represents a script inside the <code>MigrationRepository</code>
 * while it is initialized and the collector for script files is done.
 *
 * This file only includes the files metadata, currently the version number,
 * the resource name and the script name. The content of the script files is read in
 * case migration scripts are requested from the repository. For performance reasons
 * it is not done during the collector as it might not be required to read the files
 * in case the database schema is up to date.
 *
 * This class implements the {@link Comparable} interface in order to sort the
 * scripts by the version.
 *
 * @author Patrick Kranz
 */
public class ScriptFile implements Comparable {
    private final int version;
    private final String resourceName;
    private final String scriptName;

    /**
     * A constructor taken all relevant information for a script. All
     * arguments are required.
     *
     * @param version the version that this script represents. This is the number in the
     *                beginning of the file name.
     * @param resourceName the name of the resource that represents the script inside the classpath
     * @param scriptName the name of the script itself. Normally everything from the file name except
     *                   the version
     */
    public ScriptFile(int version, String resourceName, String scriptName) {
        this.version = version;
        this.resourceName = notNullOrEmpty(resourceName, "resourceName");
        this.scriptName = notNullOrEmpty(scriptName, "scriptName");
    }

    public int getVersion() {
        return version;
    }

    public String getResourceName() {
        return resourceName;
    }

    public String getScriptName() {
        return scriptName;
    }

    @Override
    public int compareTo(Object o) {
        if (o == null || !ScriptFile.class.isAssignableFrom(o.getClass())) {
            return 1;
        }
        return Integer.compare(this.version, ((ScriptFile) o).version);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !ScriptFile.class.isAssignableFrom(o.getClass())) {
            return false;
        }
        if (o == this) {
            return true;
        }

        ScriptFile other = (ScriptFile) o;
        return this.resourceName.equals(other.resourceName)
                && this.scriptName.equals(other.scriptName)
                && this.version == other.version;
    }

    @Override
    public int hashCode() {
        int result = version;
        result = 31 * result + resourceName.hashCode();
        result = 31 * result + scriptName.hashCode();
        return result;
    }
}
