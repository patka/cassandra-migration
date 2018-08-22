package org.cognitor.cassandra.migration;

import com.datastax.driver.core.*;
import org.cognitor.cassandra.migration.cql.SimpleCQLLexer;
import org.cognitor.cassandra.migration.keyspace.KeyspaceDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Date;
import java.util.List;

import static java.lang.String.format;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toList;
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
     * The name of the column that contains the checksum
     */
    private static final String CHECKSUM_COLUMN_NAME = "checksum";

    /**
     * Insert statement that logs a migration into the schema_migration table.
     */
    private static final String INSERT_MIGRATION = "insert into %s"
            + "(applied_successful, version, script_name, script, checksum, executed_at) values(?, ?, ?, ?, ?, ?)";

    /**
     * Statement used to create the table that manages the migrations.
     */
    private static final String CREATE_MIGRATION_CF = "CREATE TABLE %s"
            + " (applied_successful boolean, version int, script_name varchar, script text, "
            + CHECKSUM_COLUMN_NAME + " bigint, executed_at timestamp, PRIMARY KEY (applied_successful, version))";

    private static final String ADD_COLUMN = "ALTER TABLE %s "
            + "ADD %s %s";

    private static final String LOAD_MIGRATIONS_QUERY = "SELECT * FROM " + SCHEMA_CF + " WHERE applied_successful = true";

    private static final String UPDATE_MIGRATION_QUERY = "UPDATE %s SET script_name=?, script=?, %s=? "
            + "WHERE applied_successful = true AND version = %d";

    /**
     * The query that retrieves current schema version
     */
    private static final String VERSION_QUERY =
            "select version from %s where applied_successful = True "
                    + "order by version desc limit 1";

    private final KeyspaceDefinition keyspace;
    private final Cluster cluster;
    private final Session session;
    private final PreparedStatement logMigrationStatement;
    private final Configuration configuration;

    private PreparedStatement updateMigrationStatement;

    private StatementResultHandler statementResultHandler = new DefaultStatementResultHandler();

    public Database(Cluster cluster, Configuration configuration) {
        this.cluster = notNull(cluster, "cluster");
        this.configuration = notNull(configuration, "configuration");
        this.keyspace = configuration.getKeyspaceDefinition();
        session = cluster.connect(keyspace.getKeyspaceName());
        ensureSchemaTable();
        this.logMigrationStatement = session.prepare(format(INSERT_MIGRATION, SCHEMA_CF));
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
        return this.keyspace.getKeyspaceName();
    }

    /**
     * Makes sure the schema migration table exists. If it is not available it will be created.
     */
    private void ensureSchemaTable() {
        if (schemaTablesIsNotExisting()) {
            createSchemaTable();
        } else if (schemaTableHasNoChecksumColumn()) {
            addChecksumColumnToMigrationTable();
        }
    }

    private void addChecksumColumnToMigrationTable() {
        LOGGER.info("Adding checksum column to schema migration column family.");
        session.execute(format(ADD_COLUMN, SCHEMA_CF, CHECKSUM_COLUMN_NAME, "bigint"));
    }

    private boolean schemaTableHasNoChecksumColumn() {
        return cluster.getMetadata()
                .getKeyspace(getKeyspaceName())
                .getTable(SCHEMA_CF)
                .getColumn(CHECKSUM_COLUMN_NAME) == null;
    }

    private boolean schemaTablesIsNotExisting() {
        return cluster.getMetadata().getKeyspace(keyspace.getKeyspaceName()).getTable(SCHEMA_CF) == null;
    }

    private void createSchemaTable() {
        session.execute(format(CREATE_MIGRATION_CF, SCHEMA_CF));
    }

    /**
     * Loads all migrations that have been <b>successfully</b> applied to the
     * database. The list is sorted by version in an ascending way. If no
     * migrations are found an empty list is returned.
     *
     * @return an ascending sorted list containing all successful migrations or
     * an empty list if there are no migrations.
     */
    public List<DbMigration> loadMigrations() {
        return session.execute(LOAD_MIGRATIONS_QUERY)
                .all().stream()
                .map(this::mapRowToMigration)
                .sorted(comparingInt(DbMigration::getVersion))
                .collect(toList());
    }

    /**
     * This method can be used to update the existing migrations. Only the script name,
     * the script content and the
     * @param migration
     */
    public void updateMigration(DbMigration migration) {
        if (migration.getVersion() > getVersion()) {
            throw new IllegalArgumentException("Given migration was never executed and is not stored.");
        }
        try {
            session.execute(prepareUpdateStatement(migration)
                    .bind(migration.getScriptName(), migration.getMigrationScript(), migration.getChecksum()));
        } catch (Exception e) {
            throw new MigrationException(
                    format("Unable to update migration for version '%d'", migration.getVersion()), e);
        }
    }

    private PreparedStatement prepareUpdateStatement(DbMigration migration) {
        if (updateMigrationStatement == null) {
            updateMigrationStatement = session.prepare(format(
                    UPDATE_MIGRATION_QUERY,
                    SCHEMA_CF,
                    CHECKSUM_COLUMN_NAME,
                    migration.getVersion()));
        }
        return updateMigrationStatement;
    }

    private DbMigration mapRowToMigration(Row row) {
        int version = row.getInt("version");
        String name = row.getString("script_name");
        String script = row.getString("script");
        Date executed_at = row.getTimestamp("executed_at");
        long checksum = row.getColumnDefinitions().contains("checksum") ? row.getLong("checksum") : 0L;
        return new DbMigration(name, version, script, checksum, executed_at);
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
        SimpleCQLLexer lexer = new SimpleCQLLexer(migration.getMigrationScript());
        lexer.getCqlQueries().stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .forEach(statement -> {
                    StatementResult result = executeStatement(statement);
                    if (statementResultHandler.isError(result)) {
                        LOGGER.debug(format("'%s' decided that an error happened for statement '%s'",
                                statementResultHandler.getClass().getName(), result.getStatement()));
                        logMigration(migration, false);
                        statementResultHandler.handleError(result, migration);
                    } else {
                        statementResultHandler.handleSuccess(result, migration);
                    }
                });
        logMigration(migration, true);
        LOGGER.debug(format("Successfully applied migration %s to version %d",
                migration.getScriptName(), migration.getVersion()));
    }

    private StatementResult executeStatement(String statement) {
        SimpleStatement simpleStatement = new SimpleStatement(statement);
        simpleStatement.setConsistencyLevel(configuration.getConsistencyLevel());

        try {
            final ResultSet result = session.execute(simpleStatement);
            return StatementResult.success(statement, result.getExecutionInfo().isSchemaInAgreement());
        } catch (Exception e) {
            return StatementResult.error(statement, e);
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
                migration.getScriptName(), migration.getMigrationScript(), migration.getChecksum(), new Date());
        session.execute(boundStatement);
    }

    public Database setStatementResultHandler(StatementResultHandler statementResultHandler) {
        this.statementResultHandler = notNull(statementResultHandler, "statementResultHandler");
        return this;
    }
}
