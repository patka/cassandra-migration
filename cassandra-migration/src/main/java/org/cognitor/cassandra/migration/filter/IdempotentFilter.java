package org.cognitor.cassandra.migration.filter;

public class IdempotentFilter implements ScriptFilter {
  @Override
  public String filter(String scriptContent) {
    return scriptContent;
  }
}
