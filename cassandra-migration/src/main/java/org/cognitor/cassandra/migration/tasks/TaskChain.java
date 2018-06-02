package org.cognitor.cassandra.migration.tasks;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
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

    /**
     * Removes all tasks of the given type from the chain.
     *
     * @param clazz the type of task that is supposed to be removed. Must not be null.
     * @return the current <code>TaskChain</code> instance
     */
    public TaskChain removeTask(Class<? extends Task> clazz) {
        tasks.removeIf(t -> t.getClass().isAssignableFrom(notNull(clazz, "clazz")));
        return this;
    }

    /**
     * Adds the task instance, given as the second argument, right before
     * the first occurrence of an instance of the task class, given as the
     * first argument, to the task chain.
     * If there is no instance of the given class in the chain, the
     * task will be added at the end of the chain.
     *
     * @param taskClass the class of the task in front of which the new task should be placed. Must not be null.
     * @param taskToAdd the new task that should be added. Must not be null.
     * @return the current <code>TaskChain</code> instance
     */
    public TaskChain addBefore(Class<? extends Task> taskClass, Task taskToAdd) {
        notNull(taskClass, "taskClass");
        notNull(taskToAdd, "taskToAdd");
        addTaskAtPositionOrEnd(taskToAdd, findInstanceOfClass(tasks, taskClass));
        return this;
    }

    /**
     * Adds the task instance, given as the second argument, right after
     * the first occurrence of an instance of the task class, given as the
     * first argument, to the task chain.
     * If there is no instance of the given class in the chain, the
     * task will be added at the end of the chain.
     *
     * @param taskClass the class of the task after which the new task should be placed. Must not be null.
     * @param taskToAdd the new task that should be added. Must not be null.
     * @return the current <code>TaskChain</code> instance
     */
    public TaskChain addAfter(Class<? extends Task> taskClass, Task taskToAdd) {
        notNull(taskClass, "taskClass");
        notNull(taskToAdd, "taskToAdd");
        int position = findInstanceOfClass(tasks, taskClass);
        if (position != -1) {
            addTaskAtPositionOrEnd(taskToAdd, position + 1);
        } else {
            addTaskAtPositionOrEnd(taskToAdd, position);
        }
        return this;
    }

    /**
     * Returns true if the given instance is inside the TaskChain. False otherwise.
     * This method uses object euqality for the check. If you do not have the
     * concrete instance that could be in the list please use the method
     * that takes a <code>lava.lang.Class</code> instance instead.
     *
     * @param task the task that should be checked to be in the chain. Must not be null.
     * @return true if the task is part of the chain, false otherwise
     */
    public boolean contains(Task task) {
        return tasks.contains(notNull(task, "task"));
    }

    /**
     * Returns true if the chain contains an instance of the given class. False otherwise.
     *
     * @param task the task that should be checked to be in the chain. Must not be null.
     * @return true if the task is part of the chain, false otherwise
     */
    public boolean contains(Class<? extends Task> task) {
        return findInstanceOfClass(tasks, notNull(task, "task")) >= 0;
    }

    private void addTaskAtPositionOrEnd(Task taskToAdd, int position) {
        if (position >= 0 && position < tasks.size()) {
            tasks.add(position, taskToAdd);
        } else {
            tasks.add(taskToAdd);
        }
    }

    private static int findInstanceOfClass(List<Task> tasks, Class<? extends Task> clazz) {
        for (int i = 0; i<tasks.size(); i++) {
            if (tasks.get(i).getClass().isAssignableFrom(clazz)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the current task chain as an unmodifiable collection.
     *
     * @return the list of current tasks or an empty list if there are not tasks.
     */
    public List<Task> getTasks() {
        return unmodifiableList(tasks);
    }
}
