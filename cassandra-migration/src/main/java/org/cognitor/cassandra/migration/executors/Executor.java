package org.cognitor.cassandra.migration.executors;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import java.util.Optional;

public interface Executor {
    ResultSet execute(String statement, Object... parameters);
    ResultSet execute(String statement);
    ResultSet execute(String statement, String executionProfileName);
    boolean keyspaceExists(String keyspaceName);
    boolean tableExists(String keyspaceName, String tableName);
    ConsistencyLevel getConsistencyLevel();
    void setConsistencyLevel(ConsistencyLevel consistencyLevel);
    void close();
}
