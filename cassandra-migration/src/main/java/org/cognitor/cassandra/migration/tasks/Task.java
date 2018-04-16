package org.cognitor.cassandra.migration.tasks;

/**
 * A Task represents an action that can be performed on the database.
 * Typical examples would be MigrationTask or RecalculateChecksumTask.
 * Tasks can be chained for execution depending on the configuration that
 * is provided.
 * <br/>
 * A Task should not close the database after it is done as this could lead
 * to subsequent tasks failing their execution. The instance that executes the
 * tasks in order is supposed to open the database before any task execution
 * and close it in the end.
 *
 * @author Patrick Kranz
 */
public interface Task {
    void execute();
}
