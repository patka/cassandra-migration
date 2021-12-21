# Changelog for Cassandra-Migration
This file will contain all important changes to the project.
This project adheres to [Semantic Versioning](http://semver.org/).

## Unreleased
* Pull Request 61: Updating README to add details about spring-data-cassandra
* Updated dependencies, most important one is Spring-Boot from 1.5.21_RELEASE to 2.6.1. As the spring dependencies are marked
  as provided, it should not have any effect on the applications using this library
* Introduction of Configuration class so that no new constructors will be required when a new configuration setting is introduced.

## Released
### v2.4.0
* [v4 only] Pull Request 52: Add ability to choose execution profile (by configuration parameter) (Thanks to rvgulinski)
* Pull Request 49 & 50: Added ScriptFilter to dynamically validate or modify script content (Thanks to Alexander Dik) 

### v2.3.1
* Pull Request 46: Fixes issue 42. Multiline CQL statements are now supported. (Thanks to Jan Švajcr)

### v2.3.0
* Pull Request 43: Adding leader election. If you are running a database with version >= 2.0 cassandra-migration will
  now try to select a host that will perform the migration in case it runs on multiple hosts. (Thanks to Alexander Dejanovski)
  
### v2.2.1
* Pull Request 24: Provide a prefix for the database migration table
* Pull Request 34: Documentation improvement
* Upgrade to Spring 1.5.21 to get rid of potential security problems in the previously used version.
  As Spring is only a provided dependency and the applications using this library must configure
  the version themselves, there is no security issue for consumers of the library.
* The Spring test dependency in the spring boot starter module is now scoped with 'test' and was updated
  to version 5.0.8
* Issue 36: Schema modifications are done by checking if schema is in agreement.

### v2.2.0
* Pull Request 20: Consistency level can now be changed for the schema migration execution
* Issue 19: Comments that do not start at the beginning of the line are now supported

### v2.1.2
* Issue 18: Scripts can be found if run inside a Spring Boot fat jar
* Issue 16: Scripts can now be placed in subfolders inside the resources folder.

### v2.1.1
* Issue 15: Java functions can now be created via migration scripts

### v2.1.0
* Issue 13: You can now register your own LocationScanner implementations
* Issue 8: It is now possible to create a keyspace as part of the initial
  migration.
* Issue 9: Fixing an issue when cassandra-migration is used in an uber-jar that
  can lead to FileSystemAlreadyExistsException

### v2.0.0
* added a new interface ScriptCollector that can be used to implement different
  behaviors of which scripts are considered by the MigrationRepository
* Change in behavior: If two scripts are present that have the same
  version number the second one will no longer be ignored but an exception will
  be thrown immediately. Not sure if this is a breaking change in terms of
  semantic versioning as this did not introduce any breaking API change
* JarLocationScanner was included to be able to scan inside jars when the
  application is started with the java -jar command and the classpath is
  constructed from a manifest file (Thanks to Pavel Borsky)
#### Breaking Changes
* the package resolver was renamed to scanner (should normally not require
  consumer code changes)
* the method findResourceNames in ClassPathLocationScanner takes now an URI
  instead of an URL (should normally not require consumer code changes)

### v1.0.2
* removed the dependency to apache commons io
* added the possibility to use single line comments (Thanks to jakobnordztrom)

### v1.0.1
* Added name and url to the pom as they are required for maven central. 

### v1.0.0
* First version
