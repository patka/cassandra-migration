package org.cognitor.cassandra.migration.filter;

/**
 * Implementation of this interface can read/update content of cql scripts during migration.
 * It could be usefull if you need to modify or validate script content in runtime.
 */
public interface ScriptFilter {

  /**
   * Called for every potential script that is found during migration process.
   *
   * @param scriptContent The content of original cql script.
   * @return Content of updated script or original non-modified value.
   */
  String filter(String scriptContent);
}
