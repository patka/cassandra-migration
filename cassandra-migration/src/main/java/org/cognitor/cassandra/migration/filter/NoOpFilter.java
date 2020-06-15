package org.cognitor.cassandra.migration.filter;

public class NoOpFilter implements ScriptFilter {
  @Override
  public String filter(String scriptContent) {
    return scriptContent;
  }
}
