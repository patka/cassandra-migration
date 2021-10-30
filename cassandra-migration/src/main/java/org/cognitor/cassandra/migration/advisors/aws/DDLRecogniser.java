package org.cognitor.cassandra.migration.advisors.aws;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Needs to be extended to return if is either CREATE, ALTER or DROP
 */
public class DDLRecogniser {
    private List<DDLDetector> detectors = Arrays.asList(new KeyspaceDetector(), new TableDetector());

    public CQLDescription evaluate(String cql) {
        for(DDLDetector detector : detectors){
            CQLDescription result = detector.getResult(cql);
            if(result.isAsyncDDL) {
                return result;
            }
        }
        return CQLDescription.nonAsyncDDL();
    }

    static class CQLDescription {
        public static enum DDLType {
            CREATE,
            ALTER,
            DROP,
            RESTORE
        }

        private boolean isAsyncDDL;
        private boolean isKeyspaceDDL;
        private boolean isTableDDL;
        private String resourceName;
        private DDLType ddlType;

        public static CQLDescription nonAsyncDDL() {
            return new CQLDescription();
        }

        public static CQLDescription asyncKeyspaceDDL(String keyspaceName, DDLType ddlType) {
            CQLDescription result = new CQLDescription();
            result.isAsyncDDL = true;
            result.isKeyspaceDDL = true;
            result.resourceName = keyspaceName;
            result.ddlType = ddlType;
            return result;
        }

        public static CQLDescription asyncTableDDL(String keyspaceName, DDLType ddlType) {
            CQLDescription result = new CQLDescription();
            result.isAsyncDDL = true;
            result.isTableDDL = true;
            result.resourceName = keyspaceName;
            result.ddlType = ddlType;
            return result;
        }

        public boolean isAsyncDDL() {
            return isAsyncDDL;
        }

        public boolean isNotAsyncDDL() {
            return !isAsyncDDL;
        }

        public boolean isKeyspaceDDL() {
            return isKeyspaceDDL;
        }

        public boolean isTableDDL() {
            return isTableDDL;
        }

        public String getResourceName() {
            return resourceName;
        }

        public DDLType getDdlType() {
            return ddlType;
        }
    }

}

abstract class DDLDetector {
    Pattern pattern;
    abstract DDLRecogniser.CQLDescription getResult(String cql);
}


class KeyspaceDetector extends DDLDetector {
    //recognise these statements:
    //CREATE KEYSPACE [ IF NOT EXISTS ] keyspace_name
    //ALTER KEYSPACE keyspace_name
    //DROP KEYSPACE [ IF EXISTS ] keyspace_name

    public KeyspaceDetector() {
        pattern = Pattern.compile("\\s*(CREATE|ALTER|DROP)\\s+KEYSPACE\\s+(IF\\s+NOT\\s+EXISTS\\s+)?(\\w+|\"\\w+\")(\\s+|$)", Pattern.CASE_INSENSITIVE);
    }

    @Override
    DDLRecogniser.CQLDescription getResult(String cql) {
        Matcher matcher = pattern.matcher(cql);
        if(!matcher.find()) {
            return DDLRecogniser.CQLDescription.nonAsyncDDL();
        }
        DDLRecogniser.CQLDescription.DDLType ddlType = DDLRecogniser.CQLDescription.DDLType.valueOf(matcher.group(1).trim().toUpperCase());
        return DDLRecogniser.CQLDescription.asyncKeyspaceDDL(matcher.group(3).replaceAll("\"", ""), ddlType);
    }
}

class TableDetector extends DDLDetector {
    //recognise these statements:
    // CREATE TABLE [ IF NOT EXISTS ] table_name (...
    // ALTER TABLE table_name ...
    // RESTORE TABLE table_name FROM TABLE ...
    // DROP TABLE [ IF EXISTS ] table_name

    public TableDetector() {
        pattern = Pattern.compile("\\s*(CREATE|ALTER|RESTORE|DROP)\\s+TABLE\\s+(IF\\s+(NOT\\s+)?EXISTS\\s+)?(\\w+|\"\\w+\")(\\s+|$)", Pattern.CASE_INSENSITIVE);
    }

    @Override
    DDLRecogniser.CQLDescription getResult(String cql) {
        Matcher matcher = pattern.matcher(cql);
        if(!matcher.find()) {
            return DDLRecogniser.CQLDescription.nonAsyncDDL();
        }
        DDLRecogniser.CQLDescription.DDLType ddlType = DDLRecogniser.CQLDescription.DDLType.valueOf(matcher.group(1).trim().toUpperCase());
        return DDLRecogniser.CQLDescription.asyncTableDDL(matcher.group(4).replaceAll("\"", ""), ddlType);
    }
}
