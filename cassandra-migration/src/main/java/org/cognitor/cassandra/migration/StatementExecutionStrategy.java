package org.cognitor.cassandra.migration;

import com.datastax.driver.core.Session;

public interface StatementExecutionStrategy {
    void executeStatement(ExecutionContext context);
}
