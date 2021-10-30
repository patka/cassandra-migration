package org.cognitor.cassandra.migration.advisors;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.reflect.ClassPath;
import org.cognitor.cassandra.migration.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class ExecutionAdvisor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public Statement beforeExecute(String statement, Statement cqlStatement) {
        return cqlStatement;
    }

    public void afterExecute(String statement) {

    }

    public static List<ExecutionAdvisor> getAdvisorsFromClasspath(CqlSession session) {
        try {
            return ClassPath.from(Thread.currentThread().getContextClassLoader())
                    .getTopLevelClassesRecursive("org.cognitor.cassandra.migration")
                    .stream()
                    .map(classInfo -> {
                                try {
                                    return Class.forName(classInfo.getName());
                                } catch (ClassNotFoundException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                    )
                    .filter(aClass -> {
                        return ExecutionAdvisor.class.isAssignableFrom(aClass) && !Modifier.isAbstract(aClass.getModifiers());
                    }).map((advisorClass) -> {
                        try {
                            return (ExecutionAdvisor)advisorClass.getDeclaredConstructor(CqlSession.class).newInstance(session);
                        } catch (NoSuchMethodException e) {
                            LOGGER.error("Advisors must have a constructor with a CqlSession parameter");
                            throw new RuntimeException(e);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            LOGGER.error("Cannot initialise advisors.");
            return Collections.emptyList();
        }
    }



}
