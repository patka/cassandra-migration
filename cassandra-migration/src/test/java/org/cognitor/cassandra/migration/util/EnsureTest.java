package org.cognitor.cassandra.migration.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.cognitor.cassandra.migration.util.Ensure.notNull;
import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Patrick Kranz
 */
public class EnsureTest {
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenNullObjectGiven() {
        notNull(null, "testArgument");
    }

    @Test
    public void shouldReturnObjectWhenNotNullObjectGiven() {
        Object testObject = new Object();
        assertThat(notNull(testObject, "testObject"), is(equalTo(testObject)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenNullStringGiven() {
        notNullOrEmpty((String) null, "testString");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenNullListGiven() {
        notNullOrEmpty((List) null, "testString");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenEmptyListGiven() {
        notNullOrEmpty(new ArrayList<String>(), "testString");
    }
}