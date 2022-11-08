# Cassandra Schema Migration for Java [![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.cognitor.cassandra/cassandra-migration/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.cognitor.cassandra/cassandra-migration)

## Purpose
This library can be used to implement migrations for the Cassandra database schema inside your Java application.
The usage is oriented on the popular tools for relational databases like flyway or liquibase.

If you want to use this library with Spring Boot you can also scroll down to the description
of how to use the Spring Boot Starter.

## Datastax Driver Version 4
If you already migrated your project to version 4 of the Datastax Driver you can use the code that is
in the branch `master_v4`. You can use it by referencing the maven artifact version <current_version>_4.
The first available version is 2.2.1_v4.

There are some things to consider when using it:
* As the `Cluster` class has been removed by Datastax you have to pass a `CqlSession` instance into the
`Database` object. You should not use the session that you pass here anywhere else because:
    * as the session can or cannot be connected to a keyspace, the `Database` has to trigger a
      `USE <keyspace>` command at some point, especially if the keyspace should be created
      before the migration. That could make your session instance end up being set for a keyspace your
      application does not expect.
    * the session will be closed after the migration
* since the library will issue a `USE <keyspace>` on the session instance, the migrations scripts
should continue to work as usual. So there is no need to add fully qualified table names
to existing scripts.
* If you are using spring boot, you have to provide a name to the current `CqlSession` instance that is
supposed to be used with this library. You can do this by adding the name to the `@Bean` annotation. 
In order to make sure that this session will not be used by your application, you can
mark the application session as primary. 
Here is an example for a programmatic configuration:
```java
@Bean
@Qualifier(CassandraMigrationAutoConfiguration.CQL_SESSION_BEAN_NAME)
public CqlSession cassandraMigrationCqlSession() {
    // session creation code here
}

@Bean
@Primary
public CqlSession applicationCqlSession() {
  // session creation code here
}
```

In order to be sure to use the correct name, there is a public constant in 
`CassandraMigrationAutoConfiguration` that is called `CQL_SESSION_BEAN_NAME`. You can use that
when declaring the session bean as shown in the example.

### Spring Data Cassandra 3.X.X
If you are using spring-data-cassandra or the reactive counterpart, providing the CqlSession named `CQL_SESSION_BEAN_NAME` to be used by this library
will bypass the spring data session as it is annotated by [`@ConditionalOnMissingBean`](https://github.com/spring-projects/spring-boot/blob/fdb1010cbc75517f511d4ab82de7d8f0ee058849/spring-boot-project/spring-boot-autoconfigure/src/main/java/org/springframework/boot/autoconfigure/cassandra/CassandraAutoConfiguration.java#L74).

The easiest solution is to provide both CqlSession and mark the one used by spring-data as `@Primary` :

```java
@Bean(CassandraMigrationAutoConfiguration.CQL_SESSION_BEAN_NAME)
public CqlSession cassandraMigrationCqlSession() {
    // migration session creation code here
}

@Bean(DefaultCqlBeanNames.SESSION)
@Primary
// ensure that the keyspace is created if needed before initializing spring-data session
@DependsOn(CassandraMigrationAutoConfiguration.MIGRATION_TASK_BEAN_NAME)
public CqlSession cassandraSession(CqlSessionBuilder cqlSessionBuilder) {
    return cqlSessionBuilder.build();
}
```

### Testing
This library uses [embedded-cassandra](https://github.com/nosan/embedded-cassandra) to test migration scripts with a running Cassandra instance.
We can change easily the embedded Cassandra version used by updating the version in tests :
```
cassandra = new CassandraBuilder()
        .version("3.11.12")
        .build();
```
Since Cassandra dropped support on Windows environment after [4.0-beta3 (CASSANDRA-16171)](https://issues.apache.org/jira/browse/CASSANDRA-16171), the only solution for testing on Cassandra 4+ on Windows will be to use [TestContainers](https://www.testcontainers.org/modules/databases/cassandra/), but it requires a Docker installation.

As of writing the embedded cassandra system only works with Java 8. If you want to run the test with Java 8 you can 
set the JAVA_HOME environment variable in the shell you will execute mvn to point to a Java 8 version:

```bash
export JAVA_HOME=<path-to-java-8>
```

## Usage
Using this library is quite simple. Given that you have a configured instance of the
cluster object all you need to do is integrate the next lines in your project startup code:

```
Database database = new Database(cluster, "nameOfMyKeyspace");
MigrationTask migration = new MigrationTask(database, new MigrationRepository());
migration.migrate();
```

This assumes that all migration scripts can be found on the classpath in a folder '/cassandra/migration'.
If you put your scripts on a different classpath location you just need to pass the path in the constructor
like this:

```java
new MigrationRepository("/my/path/here");
```

## Naming
Scripts should be named in the following schema:

```
<version>_<name>.cql
```

If the `.cql` extension is missing the file will be ignored. The `version` is required to figure out the latest
version of the scripts and relates to the version that is stored in the database schema information table.
The version should start with one as an empty database is considered to have a version of
zero. Leading zeros for better sorting are accepted.
The name is something that is just for the developers purpose and should be something descriptive.

In case there are multiple scripts with the same version (duo to a merge of branches for example),
an exception is thrown immediately. This behavior can be changed by creating an instance of the
MigrationRepository providing a ScriptCollector implementation like this:
```java
new MigrationRepository(MigrationRepository.DEFAULT_SCRIPT_PATH, new IgnoreDuplicatesCollector());
```
Until the version 1.0.2 inclusive the default behavior was to ignore duplicates by considering
only the first script file for a particular version. As this can lead to unpredictable behavior, since
it is just a matter of which script is found first, this behavior is no longer the default.

## Script content
Single line comments are indicated by either '//' or '--' characters.
Multi line comments are not supported.

## Migrations
Migrations are executed with the Quorum consistency level to make sure that always a majority of nodes share the same schema information.
Besides this after the scripts are executed, it will be checked if the schema is in agreement by calling the
corresponding method on the metadata of the ResultSet. That call is blocking until either an agreement has been
reached or the configured `maxSchemaAgreementWaitSeconds` have been passed. This value can be configured on the Cluster
builder. 
Error handling is not really implemented (and as far as I know not really possible from a database point of view).
If one script fails the migration is stopped and an exception is thrown. The exception contains the name of
the failing script as well as the broken statement in case the error happened during the execution of a
statement.
Every script will result in an entry into the schema_migration table. If a script fails, an entry will be put
into the 'migration_schema' table stating that this script failed. You can then fix the script and retry the migration.
It should normally not be necessary to remove failed migrations from the 'migration_schema' table.

However, in case you have multiple statements in one script and one of them failed you need to make sure that
the statements before the failing one are safe to be executed again. You either need to manually revert
the actions or, the preferred approach, make use of Cassandras "IF EXISTS" or "IF NOT EXISTS" mechanism to
ensure that the same script can be run multiple times without failing.

## More details
The library checks if there is a table inside the given keyspace that is called "schema_migration". It will be created if it
doesn't already exist and will contain the following columns:
* applied_successful (boolean)
* version (int)
* script_name varchar
* script (text)
* executed_at (timestamp)

"applied_successful" and "version" together make the primary key. The version of the database schema is equivalent
to the highest number returned by the version column where applied_successful is true.
This means, even if your counting does not start at one (because you removed some very old scripts)
the schema version is not affected by this.

All migrations that are marked as applied_successful = false do not affect the version number in any way. It is also
perfectly legal to have the same version number once with a successful execution and one with a failing execution,
for example, if the first try failed and the script was fixed afterwards. However, you will only see the last failing
execution. If the same script fails twice the first failure will be overwritten.

If you want the migration table to be prefixed, e.g. you are using the same keyspace for multiple applications
(hint: you should not :)) and don't want all applications to write into the same migration table you can prefix the
table name. Just provide the prefix in the constructor of the Database or as a spring option (see below).
The prefix will be separated by an underscore from the "schema_migration" string, e.g. with prefix "myApp" the table
name would be "myApp_schema_migration". 

## Execution profiles
You can configure Cassandra-Migration to make use of execution profiles for the migration scripts. This can be very
useful in case the migration scripts can take a long time to run and you do not want to change your application timeouts
just for the migration of the schema. In that case you can define a separate profile with different timeouts that can be
used just for migrations. Have a look on the [Datastax documentation](https://docs.datastax.com/en/developer/java-driver/4.6/manual/core/configuration/)
on how to define such a profile.
Once defined, you can set the execution profile name in the `MigrationConfiguration` and it will be used during migration.

## Version deprecation
Please be aware that the version 2 of this library that uses the old version 3 Datastax driver will be deprecated by end
of 2021. New features will mainly be implemented on the v4 version and only backported if requested. Bugfixes will still
be applied. As this project is development only during spare time and I don't have the time to spend that I would like
to, I have to prioritize my work. Besides, even an older installation of the database itself can be used with the newer
driver so there is in general nothing preventing one from upgrading.

## Maven
If you are using maven you can add cassandra-migration as a dependency to your project like this:
```xml
  <dependency>
      <groupId>org.cognitor.cassandra</groupId>
      <artifactId>cassandra-migration</artifactId>
      <version>2.5.0_v4</version>
  </dependency>
```

## Spring Boot
Cassandra Migration comes with a Spring Boot Starter module that can be used to autoconfigure
the migration. You have to include the following dependency to make it work:
```xml
  <dependency>
      <groupId>org.cognitor.cassandra</groupId>
      <artifactId>cassandra-migration-spring-boot-starter</artifactId>
      <version>2.5.0_v4</version>
  </dependency>
```

In your properties file you can set the following properties:
* cassandra.migration.keyspace-name Specifies the keyspace that should be migrated
* cassandra.migration.simple-strategy.replication-factor Specifies the simple strategy to use on the keyspace
* cassandra.migration.network-strategy.replications Specifies the network strategy to use on the keyspace with datacenters and factors
* cassandra.migration.script-locations Overrides the default script location
* cassandra.migration.strategy Can either be IGNORE_DUPLICATES or FAIL_ON_DUPLICATES
* cassandra.migration.consistency-level Provides the consistency level that will be used to execute migrations
* cassandra.migration.table-prefix Prefix for the migrations table name
* cassandra.migration.execution-profile-name the name for the execution profile
* cassandra.migration.with-consensus to prevent concurrent schema updates.