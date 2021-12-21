package org.cognitor.cassandra.migration;

import org.cognitor.cassandra.migration.keyspace.Keyspace;

import static org.cognitor.cassandra.migration.util.Ensure.notNull;

public class MigrationConfiguration {
    public final String EMPTY_TABLE_PREFIX = "";
    private String tablePrefix = EMPTY_TABLE_PREFIX;
    private Keyspace keyspace;

    public MigrationConfiguration withKeyspaceName(String keyspaceName) {
        this.keyspace = new Keyspace(keyspaceName);
        return this;
    }

    public MigrationConfiguration withTablePrefix(String tablePrefix) {
        if (tablePrefix == null) {
            this.tablePrefix = EMPTY_TABLE_PREFIX;
        }
        this.tablePrefix = tablePrefix;
        return this;
    }

    public MigrationConfiguration withKeyspace(Keyspace keyspace) {
        this.keyspace = notNull(keyspace, "keyspace");
        return this;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public Keyspace getKeyspace() {
        return keyspace;
    }

    public boolean isValid() {
        return keyspace != null;
    }

    @Override
    public String toString() {
        return "MigrationConfiguration {" +
                " [OPTIONAL] tablePrefix='" + tablePrefix + '\'' +
                ",[REQUIRED] keyspace=" + keyspace +
                '}';
    }
}
