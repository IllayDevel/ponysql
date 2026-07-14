/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
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
