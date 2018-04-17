package org.cognitor.cassandra.migration.tasks;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Patrick Kranz
 */
public class TaskChainTest {

    @Test
    public void shouldExecuteTasksInTheGivenOrder() {
        TaskChain chain = new TaskChain();
        final AtomicInteger counter = new AtomicInteger(0);
        chain.addTask(() -> {
            if (counter.get() == 0) counter.set(1);
        });
        chain.addTask(() -> {
            if (counter.get() == 1) counter.set(2);
        });
        chain.addTask(() -> {
            if (counter.get() == 2) counter.set(3);
        });
        chain.execute();
        assertThat(counter.get(), is(equalTo(3)));
    }

    @Test
    public void shouldRemoveTaskWhenExistingTaskTypeGiven() {
        TaskChain chain = new TaskChain();
        chain.addTask(new TestTask());
        chain.addTask(() -> System.out.println("Test"));
        assertThat(chain.getTasks().size(), is(equalTo(2)));
        chain.removeTask(TestTask.class);
        assertThat(chain.getTasks().size(), is(equalTo(1)));
    }

    @Test
    public void shouldRemoveAllTasksOfTypeWhenExistingTaskTypeGiven() {
        TaskChain chain = new TaskChain();
        chain.addTask(new TestTask()).addTask(new TestTask());
        chain.addTask(() -> System.out.println("Test"));
        assertThat(chain.getTasks().size(), is(equalTo(3)));
        chain.removeTask(TestTask.class);
        assertThat(chain.getTasks().size(), is(equalTo(1)));
    }

    @Test
    public void shouldRemoveFirstTaskInstanceWhenInstanceOfExistingTaskGiven() {
        TaskChain chain = new TaskChain();
        TestTask task = new TestTask();
        chain.addTask(task).addTask(task);
        assertThat(chain.getTasks().size(), is(equalTo(2)));
        chain.removeTask(task);
        assertThat(chain.getTasks().size(), is(equalTo(1)));
    }
}

class TestTask implements Task {

    @Override
    public void execute() {
    }
}