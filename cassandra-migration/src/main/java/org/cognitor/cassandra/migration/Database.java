package org.cognitor.cassandra.migration;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import org.cognitor.cassandra.migration.cql.SimpleCQLLexer;
import org.cognitor.cassandra.migration.keyspace.KeyspaceDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Date;

import static java.lang.String.format;
import static org.cognitor.cassandra.migration.util.Ensure.notNull;
import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;

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

    private final String keyspaceName;
    private final KeyspaceDefinition keyspace;
    private final Cluster cluster;
    private final Session session;
    private final PreparedStatement logMigrationStatement;
    private Configuration configuration = new Configuration();

    public Database(Cluster cluster, KeyspaceDefinition keyspace) {
        this.cluster = notNull(cluster, "cluster");
        this.keyspace = notNull(keyspace, "keyspace");
        this.keyspaceName = keyspace.getKeyspaceName();
        createKeyspaceIfRequired();
        session = cluster.connect(keyspaceName);
        ensureSchemaTable();
        this.logMigrationStatement = session.prepare(format(INSERT_MIGRATION, SCHEMA_CF));
    }

    /**
     * Creates a new instance of the database.
     *
     * @param cluster      the cluster that is connected to a cassandra instance
     * @param keyspaceName the keyspace name that will be managed by this instance
     */
    public Database(Cluster cluster, String keyspaceName) {
        this.cluster = notNull(cluster, "cluster");
        this.keyspaceName = notNullOrEmpty(keyspaceName, "keyspaceName");
        this.keyspace = null;
        session = cluster.connect(keyspaceName);
        ensureSchemaTable();
        this.logMigrationStatement = session.prepare(format(INSERT_MIGRATION, SCHEMA_CF));
    }

    private void createKeyspaceIfRequired() {
        if (keyspaceExists()) {
            return;
        }
        try (Session session = this.cluster.connect()) {
            session.execute(this.keyspace.getCqlStatement());
        } catch (DriverException exception) {
            throw new MigrationException(format("Unable to create keyspace %s.", keyspaceName), exception);
        }
    }

    private boolean keyspaceExists() {
        return cluster.getMetadata().getKeyspace(keyspaceName) != null;
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
        ResultSet resultSet = session.execute(format(VERSION_QUERY, SCHEMA_CF));
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

    /**
     * Makes sure the schema migration table exists. If it is not available it will be created.
     */
    private void ensureSchemaTable() {
        if (schemaTablesIsNotExisting()) {
            createSchemaTable();
        }
    }

    private boolean schemaTablesIsNotExisting() {
        return cluster.getMetadata().getKeyspace(keyspaceName).getTable(SCHEMA_CF) == null;
    }

    private void createSchemaTable() {
        session.execute(format(CREATE_MIGRATION_CF, SCHEMA_CF));
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
                executeStatement(statement);
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

    private void executeStatement(String statement) {
        if (!statement.isEmpty()) {
            SimpleStatement simpleStatement = new SimpleStatement(statement);
            simpleStatement.setConsistencyLevel(configuration.getConsistencyLevel());
            session.execute(simpleStatement);
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

    public void setConfiguration(Configuration configuration) {
        this.configuration = notNull(configuration, "configuration");
    }
}
