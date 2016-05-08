package org.cognitor.cassandra.migration;

/**
 * Indicates that something went wrong during a migration. Usually this means that a
 * migration script failed.
 *
 * Take a close look on the cause as it might indicate if manual correction on the DB
 * is required.
 *
 * @author Patrick Kranz
 */
public class MigrationException extends RuntimeException {
    public MigrationException(String message, Throwable cause) {
        super(message, cause);
    }
}
