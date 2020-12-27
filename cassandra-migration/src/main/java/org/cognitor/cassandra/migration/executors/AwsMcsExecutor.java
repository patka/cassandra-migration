package org.cognitor.cassandra.migration.executors;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import org.cognitor.cassandra.migration.MigrationException;
import org.cognitor.cassandra.migration.executors.DDLRecogniser.DDLRecogniserResult;
import org.cognitor.cassandra.migration.executors.DDLRecogniser.DDLRecogniserResult.DDLType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsMcsExecutor implements Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final CqlSession session;

    private Executor delegateExecutor;
    DDLRecogniser ddlRecogniser = new DDLRecogniser();

    private static final int[] QUERY_INTERVALS_MILIS = new int[]{ 50, 100, 300, 600, 1000 };
    //MCS: countRows is not yet supported.
    private static final String KEYSPACE_OPERATION_CHECK_QUERY = "SELECT * FROM system_schema_mcs.keyspaces WHERE keyspace_name = '%s'";

    private static final String TABLE_OPERATION_CHECK_QUERY = "SELECT keyspace_name, table_name, status FROM system_schema_mcs.tables WHERE keyspace_name = '%s' AND table_name = '%s'";

    public AwsMcsExecutor(CqlSession session) {
        this.session = session;
        //aws requires LOCAL_QUORUM consistency
        delegateExecutor = new SimpleExecutor(session, DefaultConsistencyLevel.LOCAL_QUORUM);
    }

    @Override
    public ResultSet execute(String statement, Object... parameters) {
        ResultSet resultSet = delegateExecutor.execute(statement, parameters);
        waitIfNecessary(statement);
        return resultSet;
    }

    @Override
    public ResultSet execute(String statement) {
        ResultSet resultSet = delegateExecutor.execute(statement);
        waitIfNecessary(statement);
        return resultSet;
    }

    @Override
    public ResultSet execute(String statement, String executionProfileName) {
        ResultSet resultSet = delegateExecutor.execute(statement, executionProfileName);
        waitIfNecessary(statement);
        return resultSet;
    }

    @Override
    public boolean keyspaceExists(String keyspaceName) {
        return delegateExecutor.keyspaceExists(keyspaceName);
    }

    @Override
    public boolean tableExists(String keyspaceName, String tableName) {
        return delegateExecutor.tableExists(keyspaceName, tableName);
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return delegateExecutor.getConsistencyLevel();
    }

    @Override
    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        delegateExecutor.setConsistencyLevel(consistencyLevel);
    }

    @Override
    public void close() {
        delegateExecutor.close();
    }

    private void waitIfNecessary(String statement) {
        DDLRecogniserResult statementType = ddlRecogniser.evaluate(statement);
        if(statementType.isAsyncDDL()) {
            if(statementType.isKeyspaceDDL()) {
                if(statementType.getDdlType() != DDLType.CREATE) {
                    LOGGER.warn("No wait support for async " + statementType.getDdlType() + " operation. Only CREATE KEYSPACE is supported!");
                    return;
                }
                waitForKeyspaceCreation(statementType.getResourceName());
            } else if (statementType.isTableDDL()) {
                waitForTableCreation(statementType.getResourceName(), statementType.getDdlType());
            }
        }
    }

    //no support for DROP KEYSPACE
    //don't know how to detect when ALTER KEYSPACE is finished (also no support)
    private void waitForKeyspaceCreation(String resourceName) {
        int keyspaces = 0;
        LOGGER.debug("Witing for completing the creation of " + resourceName);
        long startTime = System.currentTimeMillis();
        int count = 0;
        while(keyspaces == 0) {
            //looks like keyspace name is transformed to lowercase by cassandra on AWS
            ResultSet resultSet = executeInternal(String.format(KEYSPACE_OPERATION_CHECK_QUERY, resourceName.toLowerCase()));
            keyspaces = resultSet.all().size();
            if(keyspaces > 0) {
                continue;
            }
            try{
                Thread.sleep(QUERY_INTERVALS_MILIS[count >= QUERY_INTERVALS_MILIS.length ? (QUERY_INTERVALS_MILIS.length - 1) : count]);
                count++;
            } catch (InterruptedException e) {
                throw new MigrationException("Interrupted while waiting for " + resourceName + " completion.", e);
            }
        }
        long duration = System.currentTimeMillis() - startTime;
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS", Locale.getDefault());
        LOGGER.debug(resourceName + " creation duration: " + sdf.format(new Date(duration - TimeZone.getDefault().getRawOffset())));
    }

    private void waitForTableCreation(String resourceName, DDLType ddlType) {
        long startTime = System.currentTimeMillis();
        if(ddlType == DDLType.CREATE || ddlType == DDLType.ALTER || ddlType == DDLType.RESTORE) {
            LOGGER.debug("Witing for completing the " + ddlType + " operation of " + resourceName);
            boolean active = false;
            int count = 0;
            while(active == false) {
                String query = String.format(TABLE_OPERATION_CHECK_QUERY, session.getKeyspace().get().asInternal(), resourceName.toLowerCase());
                ResultSet resultSet = executeInternal(query);
                Row row = resultSet.one();
                if(row != null) {
                    String status = row.getString("status");
                    if ("ACTIVE".equalsIgnoreCase(status)) {
                        active = true;
                        continue;
                    }
                }
                try{
                    Thread.sleep(QUERY_INTERVALS_MILIS[count >= QUERY_INTERVALS_MILIS.length ? (QUERY_INTERVALS_MILIS.length - 1) : count]);
                    count++;
                } catch (InterruptedException e) {
                    throw new MigrationException("Interrupted while waiting for " + resourceName + " completion.", e);
                }
            }
            long duration = System.currentTimeMillis() - startTime;
            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS", Locale.getDefault());
            LOGGER.debug(resourceName + " creation duration: " + sdf.format(new Date(duration - TimeZone.getDefault().getRawOffset())));
        } else if(ddlType == DDLType.DROP) {
            LOGGER.debug("Witing for completing the " + ddlType + " operation of " + resourceName);
            boolean deleted = false;
            int count = 0;
            while(deleted == false) {
                String query = String.format(TABLE_OPERATION_CHECK_QUERY, session.getKeyspace().get().asInternal(), resourceName.toLowerCase());
                ResultSet resultSet = executeInternal(query);
                Row row = resultSet.one();
                if (row == null) {
                    deleted = true;
                    continue;
                }
                try{
                    Thread.sleep(QUERY_INTERVALS_MILIS[count >= QUERY_INTERVALS_MILIS.length ? (QUERY_INTERVALS_MILIS.length - 1) : count]);
                    count++;
                } catch (InterruptedException e) {
                    throw new MigrationException("Interrupted while waiting for " + resourceName + " completion.", e);
                }
            }
            long duration = System.currentTimeMillis() - startTime;
            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS", Locale.getDefault());
            LOGGER.debug(resourceName + " creation duration: " + sdf.format(new Date(duration - TimeZone.getDefault().getRawOffset())));
        } else {
            LOGGER.warn("DDL type " + ddlType + " not handled for " + resourceName);
        }
    }

    //not using delegateExecutor to prevent logging from SimpleExecutor
    private ResultSet executeInternal(String statement) {
        SimpleStatement simpleStatement = SimpleStatement.newInstance(statement);
        LOGGER.trace("aws mcs check query: " + statement);
        return session.execute(simpleStatement);
    }
}
