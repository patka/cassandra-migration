package org.cognitor.cassandra.migration;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

public class ExecutionContext {
    private DbMigration currentDbMigration;
    private String currentStatement;
    private Session session;
    private ConsistencyLevel consistencyLevel;

    public ExecutionContext(DbMigration currentDbMigration, Session session, ConsistencyLevel consistencyLevel) {
        this.currentDbMigration = currentDbMigration;
        this.session = session;
        this.consistencyLevel = consistencyLevel;
    }

    public DbMigration getCurrentDbMigration() {
        return currentDbMigration;
    }

    public String getCurrentStatement() {
        return currentStatement;
    }

    public void setCurrentStatement(String currentStatement) {
        this.currentStatement = currentStatement;
    }

    public Session getSession() {
        return session;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }
}
