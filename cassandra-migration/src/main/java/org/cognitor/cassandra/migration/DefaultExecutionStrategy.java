package org.cognitor.cassandra.migration;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

public class DefaultExecutionStrategy implements StatementExecutionStrategy {
    @Override
    public void executeStatement(ExecutionContext context) {
        final String statement = context.getCurrentStatement();
        if (!statement.isEmpty()) {
            SimpleStatement simpleStatement = new SimpleStatement(statement);
            simpleStatement.setConsistencyLevel(context.getConsistencyLevel());
            ResultSet resultSet = context.getSession().execute(simpleStatement);
            if (!resultSet.getExecutionInfo().isSchemaInAgreement()) {
                throw new MigrationException("Schema agreement could not be reached. " +
                        "You might consider increasing 'maxSchemaAgreementWaitSeconds'.",
                        context.getCurrentDbMigration().getScriptName());
            }
        }
    }
}
