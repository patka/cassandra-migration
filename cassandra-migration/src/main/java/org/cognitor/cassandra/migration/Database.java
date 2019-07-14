package org.cognitor.cassandra.migration;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import org.cognitor.cassandra.migration.cql.SimpleCQLLexer;
import org.cognitor.cassandra.migration.keyspace.Keyspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Instant;
import java.util.Optional;

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
     * Insert statement that logs a migration into the schema_migration table.
     */
    private static final String INSERT_MIGRATION = "insert into %s"
            + "(applied_successful, version, script_name, script, executed_at) values(?, ?, ?, ?, ?)";

    /**
     * Statement used to create the table that manages the migrations.
     */
    private static final String CREATE_MIGRATION_CF = "CREATE TABLE %s"
            + " (applied_successful boolean, version int, script_name varchar, script text,"
            + " executed_at timestamp, PRIMARY KEY (applied_successful, version))";

    /**
     * The query that retrieves current schema version
     */
    private static final String VERSION_QUERY =
            "select version from %s where applied_successful = True "
                    + "order by version desc limit 1";

    /**
     * Error message that is thrown if there is an error during the migration
     */
    private static final String MIGRATION_ERROR_MSG = "Error during migration of script %s while executing '%s'";

    private final String tableName;
    private final String keyspaceName;
    private final Keyspace keyspace;
    private final CqlSession session;
    private ConsistencyLevel consistencyLevel = DefaultConsistencyLevel.QUORUM;
    private final PreparedStatement logMigrationStatement;

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
        this.tableName = createTableName(tablePrefix);
        createKeyspaceIfRequired();
        useKeyspace();
        ensureSchemaTable();
        this.logMigrationStatement = this.session.prepare(format(INSERT_MIGRATION, getTableName()));
    }

    private void useKeyspace() {
        LOGGER.info("Configuring the keyspace for the ongoing session.");
        session.execute("USE " + keyspaceName);
    }

    private static String createTableName(String tablePrefix) {
        if (tablePrefix == null || tablePrefix.isEmpty()) {
            return SCHEMA_CF;
        }
        return String.format("%s_%s", tablePrefix, SCHEMA_CF);
    }

    private void createKeyspaceIfRequired() {
        if (keyspace == null || keyspaceExists()) {
            return;
        }
        try {
            session.execute(this.keyspace.getCqlStatement());
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
        ResultSet resultSet = session.execute(format(VERSION_QUERY, getTableName()));
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
        Optional<KeyspaceMetadata> keyspace = metadata.getKeyspace(keyspaceName);
        return keyspace
                .map(keyspaceMetadata -> keyspaceMetadata.getTable(getTableName())
                        .isPresent())
                .orElse(false);
    }

    private void createSchemaTable() {
        session.execute(format(CREATE_MIGRATION_CF, getTableName()));
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
                    .setConsistencyLevel(consistencyLevel);
            ResultSet resultSet = session.execute(simpleStatement);
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
}
