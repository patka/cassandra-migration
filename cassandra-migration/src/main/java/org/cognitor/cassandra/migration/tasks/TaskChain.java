package org.cognitor.cassandra.migration.tasks;

import java.util.ArrayList;
import java.util.List;

import static org.cognitor.cassandra.migration.util.Ensure.notNull;

/**
 * A <code>TaskChain</code> is a <code>Task</code> implementation
 * that executes several tasks in a row.
 * Tasks are executed in the order they are given.
 *
 * @author Patrick Kranz
 */
public class TaskChain implements Task {
    private List<Task> tasks = new ArrayList<>();

    @Override
    public void execute() {
        tasks.forEach(Task::execute);
    }

    /**
     * Adds a task to the end of the chain.
     *
     * @param task The <code>Task</code> to be added. Must not be null.
     * @return the current <code>TaskChain</code> instance.
     */
    public TaskChain addTask(Task task) {
        tasks.add(notNull(task, "task"));
        return this;
    }

    /**
     * Removes the task from the chain of tasks. This method
     * uses object equality, so in the standard implementation
     * this only works if the same instance was added.
     *
     * @param task the <code>Task</code> to be added. Must not be null.
     * @return the current <code>TaskChain</code> instance.
     */
    public TaskChain removeTask(Task task) {
        tasks.remove(notNull(task, "task"));
        return this;
    }
}
