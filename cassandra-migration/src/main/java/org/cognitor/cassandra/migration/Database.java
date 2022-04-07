package org.cognitor.cassandra.migration;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import org.cognitor.cassandra.migration.cql.SimpleCQLLexer;
import org.cognitor.cassandra.migration.keyspace.Keyspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
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
    private static final String CREATE_MIGRATION_CF = "CREATE TABLE IF NOT EXISTS %s"
            + " (applied_successful boolean, version int, script_name varchar, script text,"
            + " executed_at timestamp, PRIMARY KEY (applied_successful, version))";

    /**
     * Statement used to create the table that manages the leader election on migrations.
     */
    private static final String CREATE_LEADER_CF = "CREATE TABLE IF NOT EXISTS %s"
            + " (keyspace_name text, leader uuid, took_lead_at timestamp, leader_hostname text, PRIMARY KEY (keyspace_name))";

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
     * The query that retrieves current schema version
     */
    private static final String VERSION_QUERY = "select version from %s where applied_successful = True "
            + "order by version desc limit 1";

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
    private final CqlSession session;
    private final ConsistencyLevel consistencyLevel = ConsistencyLevel.QUORUM;
    private final PreparedStatement logMigrationStatement;
    private final PreparedStatement takeMigrationLeadStatement;
    private final PreparedStatement releaseMigrationLeadStatement;
    private String executionProfileName;
    private ConsistencyLevel migrationConsistencyLevel = ConsistencyLevel.QUORUM;
    private boolean tookLead = false;

    /**
     * Deprecated in favour of <code>Database(CqlSession, MigrationConfiguration)</code>. This constructor
     * will be removed in future releases.
     */
    @Deprecated
    public Database(CqlSession session, Keyspace keyspace) {
        this(session, new MigrationConfiguration().withKeyspace(keyspace));
    }

    /**
     * Deprecated in favour of <code>Database(CqlSession, MigrationConfiguration)</code>. This constructor
     * will be removed in future releases.
     */
    @Deprecated
    public Database(CqlSession session, Keyspace keyspace, String tablePrefix) {
        this(session, new MigrationConfiguration().withKeyspace(keyspace).withTablePrefix(tablePrefix));
    }

    /**
     * Creates a new instance of the database.
     *
     * Deprecated in favour of <code>Database(CqlSession, MigrationConfiguration)</code>. This constructor
     * will be removed in future releases.
     *
     * @param session      the session that is connected to a cassandra instance
     * @param keyspaceName the keyspace name that will be managed by this instance
     */
    @Deprecated
    public Database(CqlSession session, String keyspaceName) {
        this(session, new MigrationConfiguration().withKeyspaceName(keyspaceName));
    }

    /**
     * Deprecated in favour of <code>Database(CqlSession, MigrationConfiguration)</code>. This constructor
     * will be removed in future releases.
     */
    @Deprecated
    public Database(CqlSession session, String keyspaceName, String tablePrefix) {
        this(session, new MigrationConfiguration().withKeyspaceName(keyspaceName).withTablePrefix(tablePrefix));
    }

    /**
     * Create a new instance of the database by using the provided <code>CqlSession</code> and
     * {@link MigrationConfiguration}. Be aware that the CqlSession might be required to change
     * the keyspace so usually you want this CqlSession to be different from the CqlSession
     * that will be used inside the application.
     *
     * The constructor will take care of creating all required tables inside the database to manage
     * versioning inside Cassandra.
     *
     * @param session the cql session that is connected to the cassandra instance. Must not be null.
     * @param configuration the configuration to be used. Must not be null and must be valid.
     */
    public Database(CqlSession session, MigrationConfiguration configuration) {
        this.session = notNull(session, "session");
        if (!configuration.isValid()) {
            throw new IllegalArgumentException("The provided configuration is invalid. Please check if all required values are" +
                    " available. Current configuration is: " + System.lineSeparator() + configuration);
        }
        this.keyspace = configuration.getKeyspace();
        this.keyspaceName = keyspace.getKeyspaceName();
        this.executionProfileName = configuration.getExecutionProfile();
        this.tableName = createTableName(configuration.getTablePrefix(), SCHEMA_CF);
        this.leaderTableName = createTableName(configuration.getTablePrefix(), SCHEMA_LEADER_CF);
        createKeyspaceIfRequired();
        useKeyspace();
        ensureSchemaTables();
        this.logMigrationStatement = this.session.prepare(format(INSERT_MIGRATION, getTableName()));
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

    private void useKeyspace() {
        LOGGER.info("Changing keyspace of the session to '{}'", keyspaceName);
        session.execute("USE " + keyspaceName);
    }

    private static String createTableName(String tablePrefix, String tableName) {
        if (tablePrefix == null || tablePrefix.isEmpty()) {
            return tableName;
        }
        return String.format("%s_%s", tablePrefix, tableName);
    }

    private void createKeyspaceIfRequired() {
        if (keyspaceExists()) {
            return;
        }
        try {
            executeStatement(this.keyspace.getCqlStatement());
        } catch (DriverException exception) {
            throw new MigrationException(format("Unable to create keyspace %s.", keyspaceName), exception);
        }
    }

    private boolean keyspaceExists() {
        return session.getMetadata().getKeyspace(keyspaceName).isPresent();
    }

    /**
     * Closes the underlying session object. The cluster will not be touched
     * and will stay open. Call this after all migrations are done.
     * After calling this, this database instance can no longer be used.
     */
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
        ResultSet resultSet = executeStatement(format(VERSION_QUERY, getTableName()));
        Row result = resultSet.one();
        if (result == null) {
            return 0;
        }
        return result.getInt(0);
    }

    public String getLeaderTableName() {
        return leaderTableName;
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

    /**
     * Makes sure the schema migration tables exist. If they are not available they will be created.
     */
    private void ensureSchemaTables() {
        if (schemaTablesIsExisting()) {
            return;
        }
        createSchemaTables();
    }

    private boolean schemaTablesIsExisting() {
        Metadata metadata = session.getMetadata();

        return isTableExisting(metadata, getTableName())
                && isTableExisting(metadata, getLeaderTableName());
    }

    private boolean isTableExisting(Metadata metadata, String tableName) {
        return metadata
                .getKeyspace(keyspaceName)
                .map(keyspaceMetadata -> keyspaceMetadata.getTable(tableName).isPresent())
                .orElse(false);
    }


    private void createSchemaTables() {
        executeStatement(format(CREATE_MIGRATION_CF, getTableName()));
        executeStatement(format(CREATE_LEADER_CF, getLeaderTableName()));
    }

    /**
     * Attempts to acquire the lead on a migration through a LightWeight
     * Transaction. For this statement the consistency level of <code>QUORUM</code> is used.
     *
     * @param repositoryLatestVersion the latest version number in the migration repository
     * @return if taking the lead succeeded.
     */
    boolean takeLeadOnMigrations(int repositoryLatestVersion) {
        while (repositoryLatestVersion > getVersion()) {
            try {
                LOGGER.debug("Trying to take lead on schema migrations");
                BoundStatement boundStatement = takeMigrationLeadStatement.bind(getKeyspaceName(), this.instanceId,
                        this.instanceAddress);
                ResultSet lwtResult = executeStatement(boundStatement, this.consistencyLevel);

                if (lwtResult.wasApplied()) {
                    LOGGER.debug("Took lead on schema migrations");
                    tookLead = true;
                    return true;
                }

                LOGGER.info("Schema migration is locked by another instance. Waiting for it to be released...");
                waitForTakeLead();
            } catch (InvalidQueryException e1) {
                // A little redundant but necessary
                LOGGER.info("All required tables do not exist yet, waiting for them to be created...");
                waitForTakeLead();
            }
        }

        return false;
    }

    private void waitForTakeLead() {
        try {
            Thread.sleep(TAKE_LEAD_WAIT_TIME);
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
            ResultSet lwtResult = executeStatement(boundStatement, this.consistencyLevel);

            if (lwtResult.wasApplied()) {
                LOGGER.debug("Released lead on schema migrations");
                tookLead = false;
                return;
            }
            // Another instance took the lead on migrations?
            // Otherwise, TTL will do the trick
            LOGGER.warn("Could not release lead on schema migrations");
        }
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
        LOGGER.debug(format("About to execute migration %s to version %d", migration.getScriptName(),
                migration.getVersion()));
        String lastStatement = null;
        try {
            SimpleCQLLexer lexer = new SimpleCQLLexer(migration.getMigrationScript());
            for (String statement : lexer.getCqlQueries()) {
                statement = statement.trim();
                lastStatement = statement;
                executeMigrationStatement(statement, migration);
            }
            logMigration(migration, true);
            LOGGER.debug(format("Successfully applied migration %s to version %d",
                    migration.getScriptName(), migration.getVersion()));
        } catch (Exception exception) {
            logMigration(migration, false);
            String errorMessage = format(MIGRATION_ERROR_MSG, migration.getScriptName(), lastStatement);
            throw new MigrationException(errorMessage, exception, migration.getScriptName(), lastStatement);
        }
    }

    private void executeMigrationStatement(String statement, DbMigration migration) {
        if (!statement.isEmpty()) {
            ResultSet resultSet = executeStatement(statement);
            if (!resultSet.getExecutionInfo().isSchemaInAgreement()) {
                throw new MigrationException("Schema agreement could not be reached. " +
                        "You might consider increasing 'maxSchemaAgreementWaitSeconds'.",
                        migration.getScriptName());
            }
        }
    }

    private ResultSet executeStatement(String statement) throws DriverException {
        return executeStatement(SimpleStatement.newInstance(statement), this.migrationConsistencyLevel);
    }

    private ResultSet executeStatement(Statement<?> statement, ConsistencyLevel consistencyLevel) throws DriverException {
        return session.execute(statement
                .setExecutionProfileName(executionProfileName)
                .setConsistencyLevel(consistencyLevel));
    }

    /**
     * Inserts the result of the migration into the migration table
     *
     * @param migration     the migration that was executed
     * @param wasSuccessful indicates if the migration was successful or not
     */
    private void logMigration(DbMigration migration, boolean wasSuccessful) {
        BoundStatement boundStatement = logMigrationStatement.bind(wasSuccessful, migration.getVersion(),
                migration.getScriptName(), migration.getMigrationScript(), Instant.now());
        executeStatement(boundStatement, this.migrationConsistencyLevel);
    }

    /**
     * Retrieve the consistency level used for migration execution.
     *
     * @return the consistency level that is used for database migrations. Never null.
     */
    public ConsistencyLevel getConsistencyLevel() {
        return migrationConsistencyLevel;
    }

    /**
     * Set the consistency level that should be used for schema upgrades. Default is <code>ConsistencyLevel.QUORUM</code>
     *
     * This method only changes the consistency level for the migration scripts and the version logging that results
     * of these scripts. For everything that happens during initialization (e.g. schema table creation)
     * the <code>ConsistencyLevel.QUORUM</code> is used.
     *
     * @param migrationConsistencyLevel the consistency level to be used. Must not be null.
     * @return the current database instance
     */
    public Database setConsistencyLevel(ConsistencyLevel migrationConsistencyLevel) {
        this.migrationConsistencyLevel = notNull(migrationConsistencyLevel, "migrationConsistencyLevel");
        return this;
    }

    /**
     * Set the execution profile name to be used for schema upgrades. This profile has to match an existing
     * profile defined in <code>application.conf</code> for the Cassandra driver. This should be set
     * in the <code>MigrationConfiguration</code> as the change here only applies to the migration scripts
     * themselves. If you want your execution profile to be used for creation of the migration table, you need
     * to set it in <code>MigrationConfiguration</code> before creating an instance of <code>Database</code>.
     *
     * @param executionProfileName the name of the profile to be used during the execution of migrations
     * @return the current database instance
     */
    public Database setExecutionProfileName(@Nullable String executionProfileName) {
        this.executionProfileName = executionProfileName;
        return this;
    }
}
