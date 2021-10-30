package org.cognitor.cassandra.migration;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import org.cognitor.cassandra.migration.advisors.ExecutionAdvisor;
import org.cognitor.cassandra.migration.advisors.ExecutionAdvisorsChain;
import org.cognitor.cassandra.migration.cql.SimpleCQLLexer;
import org.cognitor.cassandra.migration.keyspace.Keyspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

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
    private static final String INSERT_MIGRATION = "insert into %s.%s"
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
    private  String executionProfileName;
    private ConsistencyLevel consistencyLevel = DefaultConsistencyLevel.QUORUM;

    private final String logMigrationStatementExpression;
    private final PreparedStatement logMigrationStatement;
    private final String takeMigrationLeadStatementExpression;
    private final PreparedStatement takeMigrationLeadStatement;
    private final String releaseMigrationLeadStatementExpression;
    private final PreparedStatement releaseMigrationLeadStatement;

    private boolean tookLead = false;

    private ExecutionAdvisorsChain advisors;

    public Database(CqlSession session, Keyspace keyspace) {
        this(session, keyspace, "");
    }

    public Database(CqlSession session, Keyspace keyspace, String tablePrefix) {
        this(session, keyspace, null, tablePrefix);
    }

    /**
     * Creates a new instance of the database.
     *
     * @param session      the session that is connected to a cassandra instance
     * @param keyspaceName the keyspace name that will be managed by this instance
     */
    public Database(CqlSession session, String keyspaceName) {
        this(session, keyspaceName, "");
    }

    public Database(CqlSession session, String keyspaceName, String tablePrefix) {
        this(session, null, keyspaceName, tablePrefix);
    }

    private Database(CqlSession session, Keyspace keyspace, String keyspaceName, String tablePrefix) {
        this.session = notNull(session, "session");
        this.keyspace = keyspace;
        this.keyspaceName = Optional.ofNullable(keyspace).map(Keyspace::getKeyspaceName).orElse(keyspaceName);
        this.tableName = createTableName(tablePrefix, SCHEMA_CF);
        this.leaderTableName = createTableName(tablePrefix, SCHEMA_LEADER_CF);
        this.advisors = new ExecutionAdvisorsChain(session);
        createKeyspaceIfRequired();
        useKeyspace();
        ensureSchemaTable();
        this.logMigrationStatementExpression = format(INSERT_MIGRATION, this.keyspaceName, getTableName());
        this.logMigrationStatement = this.session.prepare(this.logMigrationStatementExpression);
        this.takeMigrationLeadStatementExpression = format(TAKE_LEAD_QUERY, getLeaderTableName(), LEAD_TTL);
        this.takeMigrationLeadStatement = session.prepare(takeMigrationLeadStatementExpression);
        this.releaseMigrationLeadStatementExpression = format(RELEASE_LEAD_QUERY, getLeaderTableName());
        this.releaseMigrationLeadStatement = session.prepare(releaseMigrationLeadStatementExpression);
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
        String statement = "USE " + keyspaceName;
        SimpleStatement cqlStatement = SimpleStatement.newInstance(statement);
        session.execute(advisors.beforeExecute(statement, cqlStatement));
        advisors.afterExecute(statement);
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
        try {
            String statement = this.keyspace.getCqlStatement();
            SimpleStatement cqlStatement = SimpleStatement.newInstance(statement);
            session.execute(advisors.beforeExecute(statement, cqlStatement));
            advisors.afterExecute(statement);
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
        String statement = format(VERSION_QUERY, getTableName());
        SimpleStatement getVersionQuery = SimpleStatement.newInstance(statement)
                .setConsistencyLevel(this.consistencyLevel);
        ResultSet resultSet = session.execute(advisors.beforeExecute(statement, getVersionQuery));
        advisors.afterExecute(statement);
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
     * Makes sure the schema migration table exists. If it is not available it will be created.
     */
    private void ensureSchemaTable() {
        if (schemaTablesIsExisting()) {
            return;
        }
        createSchemaTable();
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


    private void createSchemaTable() {
        String migrationStatement = format(CREATE_MIGRATION_CF, getTableName());
        SimpleStatement cqlMigrationStatement = SimpleStatement.newInstance(migrationStatement);
        session.execute(advisors.beforeExecute(migrationStatement, cqlMigrationStatement));
        advisors.afterExecute(migrationStatement);

        String leaderStatement = format(CREATE_LEADER_CF, getLeaderTableName());
        SimpleStatement cqlLeaderStatement = SimpleStatement.newInstance(leaderStatement);
        session.execute(advisors.beforeExecute(leaderStatement, cqlLeaderStatement));
        advisors.afterExecute(leaderStatement);
    }

    /**
     * Attempts to acquire the lead on a migration through a LightWeight
     * Transaction.
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
                ResultSet lwtResult = session.execute(advisors.beforeExecute(takeMigrationLeadStatementExpression, boundStatement));
                advisors.afterExecute(takeMigrationLeadStatementExpression);

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
            ResultSet lwtResult = session.execute(advisors.beforeExecute(releaseMigrationLeadStatementExpression, boundStatement));
            advisors.afterExecute(releaseMigrationLeadStatementExpression);

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
                executeStatement(statement, migration);
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

    private void executeStatement(String statement, DbMigration migration) {
        if (!statement.isEmpty()) {
            SimpleStatement simpleStatement = SimpleStatement.newInstance(statement)
                    .setExecutionProfileName(executionProfileName)
                    .setConsistencyLevel(consistencyLevel);
            ResultSet resultSet = session.execute(advisors.beforeExecute(statement, simpleStatement));
            advisors.afterExecute(statement);
            if (!resultSet.getExecutionInfo().isSchemaInAgreement()) {
                throw new MigrationException("Schema agreement could not be reached. " +
                        "You might consider increasing 'maxSchemaAgreementWaitSeconds'.",
                        migration.getScriptName());
            }
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
                migration.getScriptName(), migration.getMigrationScript(), Instant.now());
        session.execute(advisors.beforeExecute(logMigrationStatementExpression, boundStatement));
        advisors.afterExecute(logMigrationStatementExpression);
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
     * Set the execution profile name to be used for schema upgrades
     *
     * @param executionProfileName
     * @return the current database instance
     */
    public Database setExecutionProfileName(@Nullable String executionProfileName) {
        this.executionProfileName = executionProfileName;
        return this;
    }

    public List<Class<? extends ExecutionAdvisor>> getAdvisors() {
        return advisors.getAdvisors();
    }

    public void setAdvisors(List<Class<ExecutionAdvisor>> advisors) {
        this.advisors.setAdvisors(advisors);
    }

    public void addAdvisor(Class<ExecutionAdvisor> advisor) {
        this.advisors.addAdvisor(advisor);
    }

}
