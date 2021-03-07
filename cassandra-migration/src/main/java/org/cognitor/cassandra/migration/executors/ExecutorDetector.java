package org.cognitor.cassandra.migration.executors;

import com.datastax.oss.driver.api.core.CqlSession;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorDetector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final CqlSession session;

    private final String mcsSchemaName = "system_schema_mcs";
    private final String mcsClusterName = "Amazon Keyspaces";

    public ExecutorDetector(CqlSession session) {
        this.session = session;
    }

    public Executor getExecutor() {
        if(isAwsMcs()) {
            LOGGER.debug("Amazon Keyspaces cluster detected.");
            return new AwsMcsExecutor(session);
        }
        return new SimpleExecutor(session);
    }

    private boolean isAwsMcs() {
        //metadata schemas can be disabled with datastax-java-driver.advanced.metadata.schema.enabled = "false"
        return hasMcsSchema() || hasAmazonKeyspaceInClusterName();
    }

    private boolean hasMcsSchema() {
        if(!session.getMetadata().getKeyspace(mcsSchemaName).isPresent()){
            return false;
        }
        return true;
    }

    private boolean hasAmazonKeyspaceInClusterName() {
        if(!session.getMetadata().getClusterName().isPresent()) {
            return false;
        }
        return session.getMetadata().getClusterName().get().toLowerCase().contains(mcsClusterName.toLowerCase());
    }
}
