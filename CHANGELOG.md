# Changelog for Cassandra-Migration
This file will contain all important changes to the project.
This project adheres to [Semantic Versioning](http://semver.org/).

## Unreleased
* added a new interface ScriptCollector that can be used to implement different
  behaviors of which scripts are considered by the MigrationRepository
* Change in behavior: If two scripts are present that have the same
  version number the second one will no longer be ignored but an exception will
  be thrown immediately. Not sure if this is a breaking change in terms of
  semantic versioning as this did not introduce any breaking API change

## Released
### v1.0.2
* removed the dependency to apache commons io
* added the possibility to use single line comments (Thanks to jakobnordztrom)

### v1.0.1
* Added name and url to the pom as they are required for maven central. 

### v1.0.0
* First version
