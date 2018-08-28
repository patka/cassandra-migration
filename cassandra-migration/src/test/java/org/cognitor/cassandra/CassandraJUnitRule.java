package org.cognitor.cassandra;

import static org.cassandraunit.utils.EmbeddedCassandraServerHelper.DEFAULT_CASSANDRA_YML_FILE;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.CQLDataSet;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.rules.ExternalResource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

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

    private Cluster cluster;
    private final CQLDataSet dataSet;
    private final String ymlFileLocation;

    public CassandraJUnitRule() {
        this(DEFAULT_SCRIPT_LOCATION, DEFAULT_CASSANDRA_YML_FILE);
    }

    public CassandraJUnitRule(String dataSetLocation, String ymlFileLocation) {
        this.dataSet = new ClassPathCQLDataSet(dataSetLocation, TEST_KEYSPACE);
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
        // this is required so that the EmbeddedCassandraServerHelper does not fail with NPE
        this.cluster = EmbeddedCassandraServerHelper.getCluster();
        load();
    }

    @Override
    protected void after() {
        // this is required so that the EmbeddedCassandraServerHelper does not fail with NPE
        EmbeddedCassandraServerHelper.getSession();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    public Cluster getCluster() {
        return this.cluster;
    }
}
