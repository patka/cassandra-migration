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
      before the migration.
    * the session will be closed after the migration
* since the library will issue a `USE <keyspace>` on the session instance, the migrations scripts
should continue to work as usual. So there is no need to add fully qualified table names
to existing scripts.
* If you are using spring boot, you have to provide a name to the current `CqlSession` instance that is
supposed to be used with this library. You can do this by adding the name to the `@Bean` annotation. 
In order to make sure that this session will not be used by your application, you can
mark the application session as primary. 
Here is an example for a programmatic configuration:
```
@Bean
@Qualifier(CassandraMigrationAutoConfiguration.CQL_SESSION_BEAN_NAME)
public CqlSession cassandraMigrationCqlSession(CqlSessionBuilder cqlSessionBuilder) {
    return cqlSessionBuilder.build();
}

@Bean
@Primary
public CqlSession applicationCqlSession() {
  // session creation code here
}
```

In order to be sure to use the correct name, there is a a public constant in 
`CassandraMigrationAutoConfiguration` that is called `CQL_SESSION_BEAN_NAME`. You can use that
when declaring the session bean.

### Reactive cassandra driver:
There is no manually-defined CqlSession if you are using ReactiveCassandraConfiguration
```
@Configuration
@RequiredArgsConstructor
@EnableReactiveCassandraRepositories
public class CassandraConfig extends AbstractReactiveCassandraConfiguration {
....
}
```
Following trick could be used to mark existing CqlSession as primary while you still need secondary session for Cassandra Migration.
```
@Component
public class CqlSessionFactoryPostProcessor implements BeanFactoryPostProcessor {

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        BeanDefinition bd = beanFactory.getBeanDefinition("cassandraSession");
        bd.setPrimary(true);
    }
}
```

### Testing in v4
Since cassandra-unit, the library that was used to start an in memory instance of Cassandra for testing, is
not yet released with support for driver version 4, the only option at the moment to run the integration tests
is to run them against a running instance on localhost.
As the tests also test the creation of user defined functions you need to change the configuration parameter
`enabled_user_defined_functions` from false to true in the cassandra.yaml config file.

## Usage
Using this library is quite simple. Given that you have a configured instance of the
cluster object all you need to do is integrate the next lines in your projects startup code:

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
Besides this after the scripts are executed, it wil be checked if the schema is in agreement by calling the
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
The library checks if there is a table inside the given keyspace that is called "schema_migration". If it
is not existing it will be created and it contains the following columns:
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
      <version>2.4.0_v4</version>
  </dependency>
```

## Spring Boot
Cassandra Migration comes with a Spring Boot Starter module that can be used to autoconfigure
the migration. You have to include the following dependency to make it work:
```xml
  <dependency>
      <groupId>org.cognitor.cassandra</groupId>
      <artifactId>cassandra-migration-spring-boot-starter</artifactId>
      <version>2.4.0_v4</version>
  </dependency>
```

### Configuration
In your properties file you will have four new properties that can be set:
* cassandra.migration.keyspace-name Specifies the keyspace that should be migrated
* cassandra.migration.script-location Overrides the default script location
* cassandra.migration.strategy Can either be IGNORE_DUPLICATES or FAIL_ON_DUPLICATES
* cassandra.migration.consistency-level Provides the consistency level that will be used to execute migrations
* cassandra.migration.table-prefix Prefix for the the migrations table name 

### Leader election
If multiple distributed processes are started the same time it's likely that migration will fail.
Cause of the failure is race condition between the processes trying to execute schema migration.
To avoid this scenario consensus flag was introduced in release 2.3.0:
```yml
cassandra.migration.with-consensus: true
```
Flag is disabled by default to support older Cassandra versions < 2.0