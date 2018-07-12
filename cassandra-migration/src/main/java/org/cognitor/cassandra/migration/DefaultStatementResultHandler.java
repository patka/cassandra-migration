package org.cognitor.cassandra.migration;

import static java.lang.String.format;

/**
 * @author Patrick Kranz
 */
public class DefaultStatementResultHandler implements StatementResultHandler {
    /**
     * Error message that is thrown if there is an error during the migration
     */
    private static final String MIGRATION_ERROR_MSG = "Error during migration of script %s while executing '%s'";
    /**
     * Error message that is thrown if something unknown caused a problem
     */
    private static final String UNKNOWN_ERROR_MSG = "An unknown error happened with statement '%s' of script '%s'";
    /**
     * Error message that is thrown if no agreement can be reached on the schema by the nodes
     */
    private static final String NO_AGREEMENT_REACHED_MSG = "Nodes were unable to reach agreement for statement '%s' in script '%s'";

    @Override
    public boolean isError(StatementResult result) {
        return !result.isSuccessful() || !result.isAgreementReached();
    }

    @Override
    public void handleError(StatementResult result, DbMigration migration) {
        if (!result.isAgreementReached()) {
            String errorMessage = format(NO_AGREEMENT_REACHED_MSG,
                    migration.getScriptName(), result.getStatement());
            throw new MigrationException(errorMessage, migration.getScriptName(), result.getStatement());
        }

        if (!result.isSuccessful()) {
            String errorMessage = format(MIGRATION_ERROR_MSG, migration.getScriptName(), result.getStatement());
            throw new MigrationException(errorMessage, result.getException(), migration.getScriptName(), result.getStatement());
        }

        throw new MigrationException(format(UNKNOWN_ERROR_MSG, result.getStatement(), migration.getScriptName()));
    }
}
