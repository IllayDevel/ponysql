/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pony.database;

import com.pony.util.IntegerVector;

/**
 * An interface that links with a Regex library.  This interface allows
 * the database engine to use any regular expression library that this
 * interface can be implemented for.
 *
 * @author Tobias Downer
 */

public interface RegexLibrary {

    /**
     * Matches a regular expression against a string value.  If the value is
     * a match against the expression then it returns true.
     *
     * @param regular_expression the expression to match (eg. "[0-9]+").
     * @param expression_ops expression operator string that specifies various
     *   flags.  For example, "im" is like '/[expression]/im' in Perl.
     * @param value the string to test.
     */
    boolean regexMatch(String regular_expression, String expression_ops,
                       String value);

    /**
     * Performs a regular expression search on the given column of the table.
     * Returns an IntegerVector that contains the list of rows in the table that
     * matched the expression.  Returns an empty list if the expression matched
     * no rows in the column.
     *
     * @param table the table to search for matching values.
     * @param column the column of the table to search for matching values.
     * @param regular_expression the expression to match (eg. "[0-9]+").
     * @param expression_ops expression operator string that specifies various
     *   flags.  For example, "im" is like '/[expression]/im' in Perl.
     */
    IntegerVector regexSearch(Table table, int column,
                              String regular_expression, String expression_ops);

}
