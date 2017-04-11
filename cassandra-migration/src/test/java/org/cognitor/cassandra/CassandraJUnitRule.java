package org.cognitor.cassandra;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.CQLDataSet;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.rules.ExternalResource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import static org.cassandraunit.utils.EmbeddedCassandraServerHelper.DEFAULT_CASSANDRA_YML_FILE;

/**
 * Cassandra Rule class that loads data from a specified point to initialize the database.
 * This rule also increases the default timeout in case of a slow CI system.
 * 
 * @author Patrick Kranz
 *
 */
public class CassandraJUnitRule extends ExternalResource {

    public static final String TEST_KEYSPACE = "tenant_data_test";
    public static final String DEFAULT_SCRIPT_LOCATION = "cassandraTestInit.cql";

    private static final long TIMEOUT = 60000L;
    private static final String LOCALHOST = "127.0.0.1";

    private final Cluster cluster;
    private final CQLDataSet dataSet;
    private final String ymlFileLocation;

    public CassandraJUnitRule() {
        this(DEFAULT_SCRIPT_LOCATION, DEFAULT_CASSANDRA_YML_FILE);
    }

    public CassandraJUnitRule(String dataSetLocation, String ymlFileLocation) {
        dataSet = new ClassPathCQLDataSet(dataSetLocation, TEST_KEYSPACE);
        cluster = new Cluster.Builder().addContactPoints(LOCALHOST).withPort(9142).build();
        this.ymlFileLocation = ymlFileLocation;
    }

    private void load() {
        Session session = cluster.connect();
        CQLDataLoader dataLoader = new CQLDataLoader(session);
        dataLoader.load(dataSet);
        session.close();
    }

    @Override
    protected void before() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(ymlFileLocation, TIMEOUT);
        load();
    }

    @Override
    protected void after() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    public Cluster getCluster() {
        return this.cluster;
    }
}
