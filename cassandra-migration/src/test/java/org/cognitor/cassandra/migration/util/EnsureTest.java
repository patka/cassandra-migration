package org.cognitor.cassandra.migration.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.cognitor.cassandra.migration.util.Ensure.notNull;
import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Patrick Kranz
 */
public class EnsureTest {
    @Test
    public void shouldThrowExceptionWhenNullObjectGiven() {
        assertThrows(IllegalArgumentException.class, () -> notNull(null, "testArgument"));
    }

    @Test
    public void shouldReturnObjectWhenNotNullObjectGiven() {
        Object testObject = new Object();
        assertThat(notNull(testObject, "testObject"), is(equalTo(testObject)));
    }

    @Test
    public void shouldThrowExceptionWhenNullStringGiven() {
        assertThrows(IllegalArgumentException.class, () -> notNullOrEmpty((String) null, "testString"));
    }

    @Test
    public void shouldThrowExceptionWhenNullListGiven() {
        assertThrows(IllegalArgumentException.class, () -> notNullOrEmpty((List) null, "testString"));
    }

    @Test
    public void shouldThrowExceptionWhenEmptyListGiven() {
        assertThrows(IllegalArgumentException.class, () -> notNullOrEmpty(new ArrayList<String>(), "testString"));
    }
}