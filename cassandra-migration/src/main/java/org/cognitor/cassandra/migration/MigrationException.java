package org.cognitor.cassandra.migration;

/**
 * Indicates that something went wrong during a migration. Usually this means that a
 * migration script failed or the system failed to read the scripts.
 * In case there was an error with a script the exception contains the name of the script
 * that failed as well as the statement that caused the failure.
 *
 * Take a close look on the cause as it might indicate if manual correction on the DB
 * is required.
 *
 * @author Patrick Kranz
 */
public class MigrationException extends RuntimeException {
    private final String scriptName;
    private final String statement;

    public MigrationException(String message) {
        this(message, null, null);
    }

    public MigrationException(String message, String scriptName) {
        this(message, null, scriptName);
    }

    public MigrationException(String message, Throwable cause) {
        this(message, cause, null, null);
    }

    public MigrationException(String message, Throwable cause, String scriptName) {
        this(message, cause, scriptName, null);
    }

    public MigrationException(String message, Throwable cause, String scriptName, String statement) {
        super(message, cause);
        this.scriptName = scriptName;
        this.statement = statement;
    }

    /**
     * The name of the script that was involved in the error. Usually
     * the script that contains a faulty statement. If the process did
     * not arrive yet at script execution this will be null.
     *
     * @return the name of the failing script or null if there was a different kind of error
     */
    public String getScriptName() {
        return scriptName;
    }

    /**
     * The statement that caused the migration to fail. This
     * is helpful in case there is more than one statement inside a script.
     *
     * @return the failing statement or null if the exception was not thrown
     *          during statement execution
     */
    public String getStatement() {
        return statement;
    }
}
