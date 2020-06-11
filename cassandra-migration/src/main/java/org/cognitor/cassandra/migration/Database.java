package org.cognitor.cassandra.migration;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import org.cognitor.cassandra.migration.cql.SimpleCQLLexer;
import org.cognitor.cassandra.migration.keyspace.Keyspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static org.cognitor.cassandra.migration.util.Ensure.notNull;

/**
 * This class represents the Cassandra database. It is used to retrieve the current version of the database and to
 * execute migrations.
 *
 * @author Patrick Kranz
 */
public class Database implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

    /**
     * The name of the table that manages the migration scripts
     */
    private static final String SCHEMA_CF = "schema_migration";

    /**
     * The name of the table that is used for leader election on migrations
     */
    private static final String SCHEMA_LEADER_CF = "schema_migration_leader";

    /**
     * Insert statement that logs a migration into the schema_migration table.
     */
    private static final String INSERT_MIGRATION = "insert into %s"
            + "(applied_successful, version, script_name, script, executed_at) values(?, ?, ?, ?, ?)";

    /**
     * Statement used to create the table that manages the migrations.
     */
    private static final String CREATE_MIGRATION_CF = "CREATE TABLE %s %s"
            + " (applied_successful boolean, version int, script_name varchar, script text,"
            + " executed_at timestamp, PRIMARY KEY (applied_successful, version))";

    /**
     * Statement used to create the table that manages the leader election on migrations.
     */
    private static final String CREATE_LEADER_CF = "CREATE TABLE %s %s"
            + " (keyspace_name text, leader uuid, took_lead_at timestamp, leader_hostname text, PRIMARY KEY (keyspace_name))";

    /**
     * The query that retrieves current schema version
     */
    private static final String VERSION_QUERY =
            "select version from %s where applied_successful = True "
                    + "order by version desc limit 1";

    /**
     * The query that attempts to get the lead on schema migrations
     */
    private static final String TAKE_LEAD_QUERY =
            "INSERT INTO %s (keyspace_name, leader, took_lead_at, leader_hostname) VALUES (?, ?, dateOf(now()), ?) IF NOT EXISTS USING TTL %s";

    /**
     * The query that releases the lead on schema migrations
     */
    private static final String RELEASE_LEAD_QUERY = "DELETE FROM %s where keyspace_name = ? IF leader = ?";

    /**
     * Error message that is thrown if there is an error during the migration
     */
    private static final String MIGRATION_ERROR_MSG = "Error during migration of script %s while executing '%s'";

    /**
     * TTL of the inserts in the schema leader table (if consensus is used for the migration), in seconds.
     */
    private static final int LEAD_TTL = 300;

    /**
     * Wait time between attempts to take the lead on schema migrations (in milliseconds)
     */
    private static final int TAKE_LEAD_WAIT_TIME = 10000;

    private final UUID instanceId = UUID.randomUUID();
    private final String instanceAddress;
    private final String tableName;
    private final String leaderTableName;
    private final String keyspaceName;
    private final Keyspace keyspace;
    private final Cluster cluster;
    private final Session session;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.QUORUM;
    private final PreparedStatement logMigrationStatement;
    private final PreparedStatement takeMigrationLeadStatement;
    private final PreparedStatement releaseMigrationLeadStatement;
    private final VersionNumber cassandraVersion;
    private boolean tookLead = false;
    private StatementExecutionStrategy executionStrategy = new DefaultExecutionStrategy();

    public Database(Cluster cluster, Keyspace keyspace) {
        this(cluster, keyspace, "");
    }

    public Database(Cluster cluster, Keyspace keyspace, String tablePrefix) {
        this(cluster, keyspace, null, tablePrefix);
    }

    /**
     * Creates a new instance of the database.
     *
     * @param cluster      the cluster that is connected to a cassandra instance
     * @param keyspaceName the keyspace name that will be managed by this instance
     */
    public Database(Cluster cluster, String keyspaceName) {
        this(cluster, keyspaceName, "");
    }

    public Database(Cluster cluster, String keyspaceName, String tablePrefix) {
        this(cluster, null, keyspaceName, tablePrefix);
    }

    private Database(Cluster cluster, Keyspace keyspace, String keyspaceName, String tablePrefix) {
        this.cluster = notNull(cluster, "cluster");
        this.keyspace = keyspace;
        this.keyspaceName = Optional.ofNullable(keyspace).map(Keyspace::getKeyspaceName).orElse(keyspaceName);
        this.tableName = createTableName(tablePrefix, SCHEMA_CF);
        this.leaderTableName = createTableName(tablePrefix, SCHEMA_LEADER_CF);
        createKeyspaceIfRequired();
        session = cluster.connect(this.keyspaceName);
        this.cassandraVersion = cluster.getMetadata().getAllHosts().stream().map(h -> h.getCassandraVersion())
                .min(VersionNumber::compareTo).get();
        ensureSchemaTable();
        this.logMigrationStatement = session.prepare(format(INSERT_MIGRATION, getTableName()));
        this.takeMigrationLeadStatement = session.prepare(format(TAKE_LEAD_QUERY, getLeaderTableName(), LEAD_TTL));
        this.releaseMigrationLeadStatement = session.prepare(format(RELEASE_LEAD_QUERY, getLeaderTableName()));
        String tmpInstanceAddress;
        try {
            tmpInstanceAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOGGER.warn("Could not find the local host address. Using default value.");
            tmpInstanceAddress = "unknown";
        }
        this.instanceAddress = tmpInstanceAddress;
    }

    private static String createTableName(String tablePrefix, String tableName) {
        if (tablePrefix == null || tablePrefix.isEmpty()) {
            return tableName;
        }
        return String.format("%s_%s", tablePrefix, tableName);
    }

    private void createKeyspaceIfRequired() {
        if (keyspace == null || keyspaceExists()) {
            return;
        }
        try (Session session = this.cluster.connect()) {
            session.execute(this.keyspace.getCqlStatement());
        } catch (DriverException exception) {
            throw new MigrationException(format("Unable to create keyspace %s.", keyspaceName), exception);
        }
    }

    private boolean keyspaceExists() {
        return cluster.getMetadata().getKeyspace(keyspace.getKeyspaceName()) != null;
    }

    /**
     * Closes the underlying session object. The cluster will not be touched
     * and will stay open. Call this after all migrations are done.
     * After calling this, this database instance can no longer be used.
     */
    @Override
    public void close() {
        this.session.close();
    }

    /**
     * Gets the current version of the database schema. This version is taken
     * from the migration table and represent the latest successful entry.
     *
     * @return the current schema version
     */
    public int getVersion() {
        Statement getVersionQuery = new SimpleStatement(format(VERSION_QUERY, getTableName()))
                .setConsistencyLevel(this.consistencyLevel);
        ResultSet resultSet = session.execute(getVersionQuery);
        Row result = resultSet.one();
        if (result == null) {
            return 0;
        }
        return result.getInt(0);
    }

    /**
     * Returns the name of the keyspace managed by this instance.
     *
     * @return the name of the keyspace managed by this instance
     */
    public String getKeyspaceName() {
        return this.keyspaceName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getLeaderTableName() {
        return leaderTableName;
    }

    /**
     * Makes sure the schema migration table exists. If it is not available it will be created.
     */
    private void ensureSchemaTable() {
        if (schemaTablesIsNotExisting()) {
            createSchemaTable();
        }
    }

    private boolean schemaTablesIsNotExisting() {
        Metadata metadata = cluster.getMetadata();
        KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName);
        TableMetadata table = keyspace.getTable(getTableName());
        TableMetadata leaderTable = keyspace.getTable(getLeaderTableName());
        return table == null || leaderTable == null;
    }

    private void createSchemaTable() {
        // "IF NOT EXISTS" is only available starting from Cassandra 2.0
        String ifNotExistsString = isVersionAtLeastV2(cassandraVersion) ? "IF NOT EXISTS" : "";
        session.execute(format(CREATE_MIGRATION_CF, ifNotExistsString, getTableName()));
        session.execute(format(CREATE_LEADER_CF, ifNotExistsString, getLeaderTableName()));
    }

    /**
     * Executes the given migration to the database and logs the migration along with the output in the migration table.
     * In case of an error a {@link MigrationException} is thrown with the cause of the error inside.
     *
     * @param migration the migration to be executed.
     * @throws MigrationException if the migration fails
     */
    public void execute(DbMigration migration) {
        notNull(migration, "migration");
        ExecutionContext executionContext = new ExecutionContext(migration, session, consistencyLevel);
        LOGGER.debug(format("About to execute migration %s to version %d", migration.getScriptName(),
                migration.getVersion()));
        try {
            SimpleCQLLexer lexer = new SimpleCQLLexer(migration.getMigrationScript());
            for (String statement : lexer.getCqlQueries()) {
                statement = statement.trim();
                executionContext.setCurrentStatement(statement);
                executionStrategy.executeStatement(executionContext);
            }
            logMigration(migration, true);
            LOGGER.debug(format("Successfully applied migration %s to version %d",
                    migration.getScriptName(), migration.getVersion()));
        } catch (Exception exception) {
            logMigration(migration, false);
            String errorMessage = format(MIGRATION_ERROR_MSG, migration.getScriptName(), executionContext.getCurrentStatement());
            throw new MigrationException(errorMessage, exception, migration.getScriptName(), executionContext.getCurrentStatement());
        }
    }

    /**
     * Inserts the result of the migration into the migration table
     *
     * @param migration     the migration that was executed
     * @param wasSuccessful indicates if the migration was successful or not
     */
    private void logMigration(DbMigration migration, boolean wasSuccessful) {
        BoundStatement boundStatement = logMigrationStatement.bind(wasSuccessful, migration.getVersion(),
                migration.getScriptName(), migration.getMigrationScript(), new Date());
        session.execute(boundStatement);
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * Set the consistency level that should be used for schema upgrades. Default is <code>ConsistencyLevel.QUORUM</code>
     *
     * @param consistencyLevel the consistency level to be used. Must not be null.
     * @return the current database instance
     */
    public Database setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = notNull(consistencyLevel, "consistencyLevel");
        return this;
    }

    /**
     * Attempts to acquire the lead on a migration through a LightWeight
     * Transaction.
     *
     * @param repositoryLatestVersion
     *            the latest version number in the migration repository
     * @return if taking the lead succeeded
     */
    boolean takeLeadOnMigrations(int repositoryLatestVersion) {
        if (!isVersionAtLeastV2(cassandraVersion)) {
            // No LWT before Cassandra 2.0 so leader election can't happen
            return true;
        }

        while (repositoryLatestVersion > getVersion()) {
            try {
                LOGGER.debug("Trying to take lead on schema migrations");
                BoundStatement boundStatement = takeMigrationLeadStatement.bind(getKeyspaceName(), this.instanceId,
                        this.instanceAddress);
                ResultSet lwtResult = session.execute(boundStatement);

                if (lwtResult.wasApplied()) {
                    LOGGER.debug("Took lead on schema migrations");
                    tookLead = true;
                    return true;
                }

                LOGGER.info("Schema migration is locked by another instance. Waiting for it to be released...");
                waitFor(TAKE_LEAD_WAIT_TIME);
            } catch (InvalidQueryException e1) {
                // A little redundant but necessary
                LOGGER.info("All required tables do not exist yet, waiting for them to be created...");
                waitFor(TAKE_LEAD_WAIT_TIME);
            }
        }

        return false;
    }

    private void waitFor(int waitTime) {
        try {
            Thread.sleep(waitTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /**
     * Attempts to release the lead on schema migrations, if it was taken by the
     * local process.
     */
    void removeLeadOnMigrations() {
        if (tookLead) {
            LOGGER.debug("Trying to release lead on schema migrations");

            BoundStatement boundStatement = releaseMigrationLeadStatement.bind(getKeyspaceName(), this.instanceId);
            ResultSet lwtResult = session.execute(boundStatement);

            if (lwtResult.wasApplied()) {
                LOGGER.debug("Released lead on schema migrations");
                tookLead = false;
                return;
            }
            // Another instance took the lead on migrations?
            // Otherwise, TTL will do the trick
            LOGGER.warn("Could not release lead on schema migrations");
            return;
        }
    }

    /**
     * Check if the Cassandra version is 2.0 or more
     *
     * @param cassandraVersion the version of Cassandra we're testing against
     * @return true if version is &gt;= 2.0, false if not
     */
    public boolean isVersionAtLeastV2(VersionNumber cassandraVersion) {
        return cassandraVersion.compareTo(VersionNumber.parse("2.0")) >= 0;
    }

    public void setExecutionStrategy(StatementExecutionStrategy strategy) {
        this.executionStrategy = notNull(strategy, "strategy");
    }
}
