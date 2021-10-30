package org.cognitor.cassandra.migration.advisors;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ExecutionAdvisorsChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private List<ExecutionAdvisor> advisors = new ArrayList<>();
    CqlSession session;

    public ExecutionAdvisorsChain(CqlSession session) {
        this.advisors.addAll(ExecutionAdvisor.getAdvisorsFromClasspath(session));
        this.session = session;
        LOGGER.debug("Loaded " + this.advisors.size() + " advisors: " + this.advisors.stream().map(adv -> adv.getClass().getSimpleName()).collect( Collectors.joining( "," ) ));
    }

    public Statement beforeExecute(String statement, Statement cqlStatement) {
        for(ExecutionAdvisor advisor : advisors) {
            cqlStatement = advisor.beforeExecute(statement, cqlStatement);
        }
        return cqlStatement;
    }

    public void afterExecute(String statement) {
        for(ExecutionAdvisor advisor : advisors) {
            advisor.afterExecute(statement);
        }
    }

    public List<Class<? extends ExecutionAdvisor>> getAdvisors() {
        return advisors.stream().map(adv -> adv.getClass()).collect(Collectors.toList());
    }

    public void setAdvisors(List<Class<ExecutionAdvisor>> advisors) {
        this.advisors.clear();
        for(Class<ExecutionAdvisor> adv : advisors) {
            addAdvisor(adv);
        }
    }

    public void addAdvisor(Class<ExecutionAdvisor> advisor) {
        try {
            this.advisors.add((ExecutionAdvisor) advisor.getDeclaredConstructor(CqlSession.class).newInstance(session));
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }




}
