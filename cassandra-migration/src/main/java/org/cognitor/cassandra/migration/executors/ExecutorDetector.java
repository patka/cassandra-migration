package org.cognitor.cassandra.migration.executors;

import com.datastax.oss.driver.api.core.CqlSession;

public class ExecutorDetector {
    private final CqlSession session;

    private final String mcsSchemaName = "system_schema_mcs";
    private final String mcsClusterName = "Amazon Keyspaces";

    public ExecutorDetector(CqlSession session) {
        this.session = session;
    }

    public Executor getExecutor() {
        if(isAwsMcs()) {
            return new AwsMcsExecutor(session);
        }
        return new SimpleExecutor(session);
    }

    private boolean isAwsMcs() {
        if(!session.getMetadata().getKeyspace(mcsSchemaName).isPresent()){
            return false;
        }
        if(!session.getMetadata().getClusterName().isPresent()) {
            return false;
        }
        return session.getMetadata().getClusterName().get().equalsIgnoreCase(mcsClusterName);
    }
}
