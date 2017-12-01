package org.cognitor.cassandra.migration;

import com.datastax.driver.core.ConsistencyLevel;

import static org.cognitor.cassandra.migration.util.Ensure.notNull;

/**
 * This class is supposed to contain different options to tune
 * the way migrations are executed. It comes with reasonable
 * default settings that should ensure smooth migrations.
 *
 * @author Patrick Kranz
 */
public class Configuration {
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.QUORUM;

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public Configuration setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = notNull(consistencyLevel, "consistencyLevel");
        return this;
    }
}
