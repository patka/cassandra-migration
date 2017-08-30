# Changelog for Cassandra-Migration
This file will contain all important changes to the project.
This project adheres to [Semantic Versioning](http://semver.org/).

## Known Issues:
* Issue 14: Java functions are wrongly interpreted as statements by its own

## Unreleased

## Released
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
