package org.cognitor.cassandra.migration.advisors.aws;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import org.cognitor.cassandra.migration.MigrationException;
import org.cognitor.cassandra.migration.advisors.ExecutionAdvisor;
import org.cognitor.cassandra.migration.advisors.aws.DDLRecogniser.CQLDescription;
import org.cognitor.cassandra.migration.advisors.aws.DDLRecogniser.CQLDescription.DDLType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import static java.lang.String.format;

public class AwsKeyspacesAdvisor extends ExecutionAdvisor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String MCS_SCHEMA_NAME = "system_schema_mcs";
    private static final String MCS_CLUSTER_NAME = "Amazon Keyspaces";

    private final CqlSession session;

    DDLRecogniser ddlRecogniser = new DDLRecogniser();
    private boolean isRunningInAws = false;

    private static final int[] QUERY_INTERVALS_MILIS = new int[]{ 50, 100, 300, 600, 1000 };
    //MCS: countRows is not yet supported.
    private static final String KEYSPACE_OPERATION_CHECK_QUERY = "SELECT * FROM system_schema_mcs.keyspaces WHERE keyspace_name = '%s'";

    private static final String TABLE_OPERATION_CHECK_QUERY = "SELECT keyspace_name, table_name, status FROM system_schema_mcs.tables WHERE keyspace_name = '%s' AND table_name = '%s'";

    private enum OperationStatus {
        RESOURCE_NOT_PRESENT,
        RESOURCE_ACTIVE,
        RESOURCE_NOT_READY
    }

    public AwsKeyspacesAdvisor(CqlSession session) {
        this.session = session;
        this.isRunningInAws = isRunningInAws();
    }

    @Override
    public Statement beforeExecute(String statement, Statement cqlStatement) {
        if(!isRunningInAws) {
            return super.beforeExecute(statement, cqlStatement);
        }
        return cqlStatement.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
    }

    @Override
    public void afterExecute(String statement) {
        if(!isRunningInAws) {
            return;
        }
        waitIfNecessary(statement);
    }

    private void waitIfNecessary(String statement) {
        CQLDescription cqlDescription = ddlRecogniser.evaluate(statement);
        if(cqlDescription.isAsyncDDL()) {
            if(cqlDescription.isKeyspaceDDL()) {
                if(cqlDescription.getDdlType() != DDLType.CREATE) {
                    LOGGER.warn("No wait support for async " + cqlDescription.getDdlType() + " operation. Only CREATE KEYSPACE is supported!");
                    return;
                }
                waitForKeyspaceCreation(cqlDescription);
            } else if (cqlDescription.isTableDDL()) {
                waitForTableOperation(cqlDescription);
            }
        }
    }

    //no support for DROP KEYSPACE
    //don't know how to detect when ALTER KEYSPACE is finished (also no support)
    private void waitForKeyspaceCreation(CQLDescription cqlDescription) {
        int keyspaces = 0;
        LOGGER.debug(format("Waiting for completing the %s KEYSPACE of %s" ,cqlDescription.getDdlType(), cqlDescription.getResourceName()));
        long startTime = System.currentTimeMillis();
        int count = 0;
        while(keyspaces == 0) {
            //looks like keyspace name is transformed to lowercase by cassandra on AWS
            ResultSet resultSet = executeInternal(format(KEYSPACE_OPERATION_CHECK_QUERY, cqlDescription.getResourceName().toLowerCase()));
            keyspaces = resultSet.all().size();
            if(keyspaces > 0) {
                continue;
            }
            incrementalSleep(count, cqlDescription);
            count++;
        }
        logStatementDuration(startTime, cqlDescription);
    }

    private void waitForTableOperation(CQLDescription cqlDescription) {
        if(cqlDescription.getDdlType() == DDLType.CREATE || cqlDescription.getDdlType() == DDLType.ALTER || cqlDescription.getDdlType() == DDLType.RESTORE) {
            waitForTableStatus(OperationStatus.RESOURCE_ACTIVE, cqlDescription);
        } else if(cqlDescription.getDdlType() == DDLType.DROP) {
            waitForTableStatus(OperationStatus.RESOURCE_NOT_PRESENT, cqlDescription);
        } else {
            LOGGER.warn("DDL type " + cqlDescription.getDdlType() + "it will not be handled for resource " + cqlDescription.getResourceName());
        }
    }

    private void waitForTableStatus(OperationStatus operationStatus, CQLDescription cqlDescription) {
        LOGGER.debug(format("Waiting for completing the %s TABLE operation of %s.", cqlDescription.getDdlType(), cqlDescription.getResourceName()));
        long startTime = System.currentTimeMillis();
        OperationStatus status = null;
        int count = 0;
        while(status != operationStatus) {
            status = getTableStatus(cqlDescription.getResourceName());
            if (status == operationStatus) {
                continue;
            }
            incrementalSleep(count, cqlDescription);
            count++;
        }
        logStatementDuration(startTime, cqlDescription);
    }

    private OperationStatus getTableStatus(String tableName) {
        String query = format(TABLE_OPERATION_CHECK_QUERY, session.getKeyspace().get().asInternal(), tableName.toLowerCase());
        ResultSet resultSet = executeInternal(query);
        Row row = resultSet.one();
        if(row == null) {
            return OperationStatus.RESOURCE_NOT_PRESENT;
        }
        if("ACTIVE".equalsIgnoreCase(row.getString("status"))) {
            return OperationStatus.RESOURCE_ACTIVE;
        }
        return OperationStatus.RESOURCE_NOT_READY;
    }

    private void logStatementDuration(long startTime, CQLDescription cqlDescription) {
        long duration = System.currentTimeMillis() - startTime;
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS", Locale.getDefault());
        LOGGER.debug(format("%s duration of %s: %s", cqlDescription.getDdlType(), cqlDescription.getResourceName(), sdf.format(new Date(duration - TimeZone.getDefault().getRawOffset()))));
    }

    private void incrementalSleep(int count, CQLDescription cqlDescription) {
        try{
            Thread.sleep(QUERY_INTERVALS_MILIS[count >= QUERY_INTERVALS_MILIS.length ? (QUERY_INTERVALS_MILIS.length - 1) : count]);
        } catch (InterruptedException e) {
            throw new MigrationException(format("Interrupted while waiting for completion of %s %s statement of %s.", cqlDescription.getDdlType(), cqlDescription.isKeyspaceDDL() ? "KEYSPACE" : "TABLE", cqlDescription.getResourceName()), e);
        }
    }

    private ResultSet executeInternal(String statement) {
        SimpleStatement simpleStatement = SimpleStatement.newInstance(statement);
        LOGGER.trace("aws mcs check query: " + statement);
        return session.execute(simpleStatement);
    }

    private boolean isRunningInAws() {
        //metadata schemas can be disabled with datastax-java-driver.advanced.metadata.schema.enabled = "false"
        return hasMcsSchema() || hasAmazonKeyspaceInClusterName();
    }

    private boolean hasMcsSchema() {
        if(!session.getMetadata().getKeyspace(MCS_SCHEMA_NAME).isPresent()){
            return false;
        }
        return true;
    }

    private boolean hasAmazonKeyspaceInClusterName() {
        if(!session.getMetadata().getClusterName().isPresent()) {
            return false;
        }
        return session.getMetadata().getClusterName().get().toLowerCase().contains(MCS_CLUSTER_NAME.toLowerCase());
    }
}
