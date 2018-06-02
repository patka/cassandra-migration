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

    @Test
    public void shouldAddTaskAfterExistingTaskWhenNewTaskGiven() {
        TaskChain chain = new TaskChain();
        TestTask task = new TestTask();
        TestTask anotherTask = new TestTask();
        chain.addTask(task).addTask(() -> {});
        chain.addAfter(TestTask.class, anotherTask);
        assertThat(chain.getTasks().size(), is(equalTo(3)));
        assertThat(chain.getTasks().get(0), is(equalTo(task)));
        assertThat(chain.getTasks().get(1), is(equalTo(anotherTask)));
    }

    @Test
    public void shouldAddTaskAtEndWhenNewTaskAndExistingTaskAtEndGiven() {
        TaskChain chain = new TaskChain();
        TestTask task = new TestTask();
        TestTask anotherTask = new TestTask();
        chain.addTask(task);
        chain.addAfter(TestTask.class, anotherTask);
        assertThat(chain.getTasks().size(), is(equalTo(2)));
        assertThat(chain.getTasks().get(0), is(equalTo(task)));
        assertThat(chain.getTasks().get(1), is(equalTo(anotherTask)));
    }

    @Test
    public void shouldAddTaskBeforeExistingTaskWhenNewTaskGiven() {
        TaskChain chain = new TaskChain();
        TestTask task = new TestTask();
        TestTask anotherTask = new TestTask();
        chain.addTask(task).addTask(() -> {});
        chain.addBefore(TestTask.class, anotherTask);
        assertThat(chain.getTasks().size(), is(equalTo(3)));
        assertThat(chain.getTasks().get(0), is(equalTo(anotherTask)));
        assertThat(chain.getTasks().get(1), is(equalTo(task)));
    }

    @Test
    public void shouldAddTaskAtEndWhenNewTaskAndNoExistingTaskGiven() {
        TaskChain chain = new TaskChain();
        TestTask task = new TestTask();
        TestTask anotherTask = new TestTask();
        chain.addTask(task).addTask(() -> {});
        chain.addAfter(MigrationTask.class, anotherTask);
        assertThat(chain.getTasks().size(), is(equalTo(3)));
        assertThat(chain.getTasks().get(0), is(equalTo(task)));
        assertThat(chain.getTasks().get(2), is(equalTo(anotherTask)));
    }

    @Test
    public void shouldReturnTrueWhenTaskInstanceInChainGiven() {
        TaskChain chain = new TaskChain();
        TestTask task = new TestTask();
        chain.addTask(task);
        assertThat(chain.contains(task), is(true));
    }

    @Test
    public void shouldReturnFalseWhenTaskInstanceNotInChainGiven() {
        TaskChain chain = new TaskChain();
        TestTask task = new TestTask();
        chain.addTask(task);
        assertThat(chain.contains(new TestTask()), is(false));
    }

    @Test
    public void shouldReturnTrueWhenTaskClassInChainGiven() {
        TaskChain chain = new TaskChain();
        TestTask task = new TestTask();
        chain.addTask(task);
        assertThat(chain.contains(TestTask.class), is(true));
    }

    @Test
    public void shouldReturnFalseWhenTaskClassNotInChainGiven() {
        TaskChain chain = new TaskChain();
        TestTask task = new TestTask();
        chain.addTask(task);
        assertThat(chain.contains(AnotherTask.class), is(false));
    }
}

class TestTask implements Task {

    @Override
    public void execute() {
    }
}

class AnotherTask implements Task {
    @Override
    public void execute() {
    }
}