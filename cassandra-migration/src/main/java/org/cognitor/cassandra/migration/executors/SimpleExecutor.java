package org.cognitor.cassandra.migration.executors;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.lang.invoke.MethodHandles;
import org.cognitor.cassandra.migration.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final CqlSession session;
    private ConsistencyLevel consistencyLevel;

    public SimpleExecutor(CqlSession session) {
        this(session, DefaultConsistencyLevel.QUORUM);
    }

    public SimpleExecutor(CqlSession session, ConsistencyLevel consistencyLevel) {
        this.session = session;
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public ResultSet execute(String statement, Object... parameters) {
        PreparedStatement preparedStatement = this.session.prepare(statement);
        BoundStatement boundStatement = preparedStatement.bind(parameters).setConsistencyLevel(consistencyLevel);
        LOGGER.debug("Executing: " + statement);
        return session.execute(boundStatement);
    }

    @Override
    public ResultSet execute(String statement) {
        return execute(statement, (String)null);
    }

    @Override
    public ResultSet execute(String statement, String executionProfileName) {
        SimpleStatement simpleStatement = SimpleStatement.newInstance(statement)
                .setConsistencyLevel(this.consistencyLevel);
        if(executionProfileName != null) {
            simpleStatement.setExecutionProfileName(executionProfileName);
        }
        LOGGER.debug("Executing: " + statement);
        return session.execute(simpleStatement);
    }

    @Override
    public boolean keyspaceExists(String keyspaceName) {
        return session.getMetadata().getKeyspace(keyspaceName).isPresent();
    }

    @Override
    public boolean tableExists(String keyspaceName, String tableName) {
        return session.getMetadata()
                .getKeyspace(keyspaceName)
                .map(keyspaceMetadata -> keyspaceMetadata.getTable(tableName).isPresent())
                .orElse(false);
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    @Override
    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public void close() {
        session.close();
    }
}
