package org.cognitor.cassandra.migration.cql;

import java.util.ArrayList;
import java.util.List;

/**
 * The original code was written and published by Steve Nicolai in his
 * <a href="https://github.com/jsevellec/cassandra-unit>cassandra-unit</a> project.
 * To be complient with this license. This code inside this class therefore is licensed
 * in the context of the LGPL license as well. The license text can be found in the
 * root folder of this project.
 *
 * see the full CQL grammar at:
 * https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/cql3/Cql.g
 *
 * This parser a series of lines, removes comments and breaks the lines into statements
 * at semicolon boundaries.
 */
public class SimpleCQLLexer {
    private enum LexState {
        DEFAULT,
        IN_SINGLE_LINE_COMMENT,
        IN_MULTI_LINE_COMMENT,
        IN_QUOTE_STRING,
        IN_SQUOTE_STRING;
    }

    private final String text;
    private LexState state;
    private int pos;

    public SimpleCQLLexer(String scriptText) {
        state = LexState.DEFAULT;
        text = scriptText;
        pos = 0;
    }

    public List<String> getCqlQueries() {
        List<String> statements = new ArrayList<>();
        StringBuilder statementUnderConstruction = new StringBuilder();

        char c;
        while ((c = getChar()) != 0) {
            switch (state) {
                case DEFAULT:
                    if (c == '/' && peekAhead() == '/') {
                        state = LexState.IN_SINGLE_LINE_COMMENT;
                        advance();
                    } else if (c == '-' && peekAhead() == '-') {
                        state = LexState.IN_SINGLE_LINE_COMMENT;
                        advance();
                    } else if (c == '/' && peekAhead() == '*') {
                        state = LexState.IN_MULTI_LINE_COMMENT;
                        advance();
                    } else if (c == '\n') {
                        statementUnderConstruction.append(' ');
                    } else {
                        statementUnderConstruction.append(c);
                        if (c == '\"') {
                            state = LexState.IN_QUOTE_STRING;
                        } else if (c == '\'') {
                            state = LexState.IN_SQUOTE_STRING;
                        } else if (c == ';') {
                            statements.add(statementUnderConstruction.toString().trim());
                            statementUnderConstruction.setLength(0);
                        }
                    }
                    break;

                case IN_SINGLE_LINE_COMMENT:
                    if (c == '\n') {
                        state = LexState.DEFAULT;
                    }
                    break;

                case IN_MULTI_LINE_COMMENT:
                    if (c == '*' && peekAhead() == '/') {
                        state = LexState.DEFAULT;
                        advance();
                    }
                    break;

                case IN_QUOTE_STRING:
                    statementUnderConstruction.append(c);
                    if (c == '"') {
                        if (peekAhead() == '"') {
                            statementUnderConstruction.append(getChar());
                        } else {
                            state = LexState.DEFAULT;
                        }
                    }
                    break;

                case IN_SQUOTE_STRING:
                    statementUnderConstruction.append(c);
                    if (c == '\'') {
                        if (peekAhead() == '\'') {
                            statementUnderConstruction.append(getChar());
                        } else {
                            state = LexState.DEFAULT;
                        }
                    }
                    break;
            }
        }
        String tmp = statementUnderConstruction.toString().trim();
        if (tmp.length() > 0) {
            statements.add(tmp);
        }

        return statements;
    }

    private char getChar() {
        if (pos < text.length()) {
            return text.charAt(pos++);
        } else {
            return 0;
        }
    }

    private char peekAhead() {
        if (pos < text.length()) {
            // don't advance
            return text.charAt(pos);
        } else {
            return 0;
        }
    }

    private void advance() {
        pos++;
    }
}