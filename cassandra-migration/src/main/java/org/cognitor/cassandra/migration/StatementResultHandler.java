package org.cognitor.cassandra.migration;

/**
 * <p>
 * The <code>StatementResultHandler</code> is used during the execution of
 * migration scripts to determine if something went wrong and if
 * yes to act accordingly. Normally you should not need to implement
 * this by yourself but if you run into a situation where you might
 * want to ignore certain problems on purpose, this is a hook to do so.
 * However, be aware, that schema migration scripts should normally not rely on this.
 * They should never make assumptions about something being in existence as
 * these scripts are supposed to be the single source of truth for your
 * database schema.
 * </p>
 * <p>
 * The database normally uses the <code>DefaultStatementResultHandler</code>
 * that throws an Exception if a statement was not able to complete
 * successfully or left the database in a state without schema
 * agreement.
 * </p>
 * <p>
 * In case you really need to change this behavior, here is how it works:
 * <ol>
 *     <li>Every migration script is divided into single statements that are
 *     are executed against the database.</li>
 *     <li>After the execution a <code>StatementResult</code> is created
 *     and passed to the <code>isError</code> method to determine if
 *     the statement was successful or not.</li>
 *     <li>In case it was successful, the next statement is executed.</li>
 *     <li>In case <code>isError</code> returns true, execution of further
 *     statements is stopped and an error is logged into the migration
 *     schema table for the current migration script.</li>
 *     <li>finally, <code>handleError</code> is called that is supposed to
 *     handle the error in whatever way and finally throw a <code>MigrationException</code>
 *     providing details for the problem.</li>
 * </ol>
 * In case <code>isError</code> is true and <code>handleError</code> does not throw an
 * exception in order to prevent further migration execution, the Database object will do this.
 * </p>
 * @author Patrick Kranz
 */
public interface StatementResultHandler {
    /**
     * Determines if the given {@link StatementResult} contains an error condition.
     * If you want to ignore some kinds of error, you should return <code>false</code> here.
     *
     * @param result the result of the statement that was executed last.
     * @return true, if the statement produced an error condition and further execution
     *          should be prohibited, false otherwise.
     */
    boolean isError(StatementResult result);

    /**
     * Handles the previously identified error. Normally this method should throw
     * a {@link MigrationException} to indicate that something went wrong and
     * stop further scripts from being executed.
     *
     * @param result the result of the failed statement execution.
     * @param migration the migration script in which context the statement failed
     */
    void handleError(StatementResult result, DbMigration migration);
}
