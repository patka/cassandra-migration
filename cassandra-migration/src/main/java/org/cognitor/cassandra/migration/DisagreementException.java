package org.cognitor.cassandra.migration;

public class DisagreementException extends Exception {
    public DisagreementException(){
        super("Could not reach a schema agreement in time.");
    }
}
