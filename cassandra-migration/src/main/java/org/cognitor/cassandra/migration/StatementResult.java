package org.cognitor.cassandra.migration;

/**
 * <p>
 * This class represents the result of a single statement
 * executed against the Cassandra database. A statement is considered
 * successful if no exception occurred during migration. Besides this
 * the information can be recorded if the cluster was able to
 * reach an agreement on the schema.
 * </p>
 * <p>
 * In case no schema agreement is reached it is up to the {@link StatementResultHandler}
 * to decide if this is considered to be an error or not.
 * </p>
 * @author Patrick Kranz
 */
public class StatementResult {
    private final boolean agreementReached;
    private final String statement;
    private final Exception exception;

    private StatementResult(String statement, boolean agreementReached) {
        this.agreementReached = agreementReached;
        this.statement = statement;
        this.exception = null;
    }

    private StatementResult(String statement, Exception exception) {
        this.exception = exception;
        this.statement = statement;
        agreementReached = false;
    }

    public boolean isAgreementReached() {
        return this.agreementReached;
    }

    public boolean isSuccessful() {
        return this.exception == null;
    }

    public Exception getException() {
        return this.exception;
    }

    public String getStatement() {
        return statement;
    }

    public static StatementResult success(String statement, boolean agreementReached) {
        return new StatementResult(statement, agreementReached);
    }

    public static StatementResult error(String statement, Exception exception) {
        return new StatementResult(statement, exception);
    }
}
