package org.cognitor.cassandra.migration.util;

/**
 * @author Patrick Kranz
 */
public final class Ensure {
    private Ensure() {
    }

    /**
     * Checks if the given argument is null and throws an exception with a
     * message containing the argument name if that it true.
     *
     * @param argument     the argument to check for null
     * @param argumentName the name of the argument to check.
     *                     This is used in the exception message.
     * @param <T>          the type of the argument
     * @return the argument itself
     * @throws IllegalArgumentException in case argument is null
     */
    public static <T> T notNull(T argument, String argumentName) {
        if (argument == null) {
            throw new IllegalArgumentException("Argument " + argumentName + " must not be null.");
        }
        return argument;
    }

    /**
     * Checks if the given String is null or contains only whitespaces.
     * The String is trimmed before the empty check.
     *
     * @param argument     the String to check for null or emptiness
     * @param argumentName the name of the argument to check.
     *                     This is used in the exception message.
     * @return the String that was given as argument
     * @throws IllegalArgumentException in case argument is null or empty
     */
    public static String notNullOrEmpty(String argument, String argumentName) {
        if (argument == null || argument.trim().isEmpty()) {
            throw new IllegalArgumentException("Argument " + argumentName + " must not be null or empty.");
        }
        return argument;
    }
}
