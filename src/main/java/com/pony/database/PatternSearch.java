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
import com.pony.util.BlockIntegerList;
import com.pony.util.IntegerIterator;


/**
 * This is a static class that performs the operations to do a pattern search
 * on a given column of a table.  The pattern syntax is very simple and follows
 * that of the SQL standard.
 * <p>
 * It works as follows:
 *   The '%' character represents any sequence of characters.
 *   The '_' character represents some character.
 * <p>
 * Therefore, the pattern search 'Toby%' will find all rows that start with
 * the string 'Toby' and end with any sequence of characters.  The pattern
 * 'T% Downer%' will find all names starting with T and containing 'Downer'
 * somewhere in the end.  The pattern '_at' will find all three letter words
 * ending with 'at'.
 * <p>
 * NOTE: A 'ab%' type search is faster than a '%bc' type search.  If the start
 *   of the search pattern is unknown then the entire contents of the column
 *   need to be accessed.
 *
 * @author Tobias Downer
 */

public final class PatternSearch {

    /**
     * Statics for the tokens.
     */
    private final static char ZERO_OR_MORE_CHARS = '%';
    private final static char ONE_CHAR = '_';


    public static boolean testSearch(String pattern, String expression,
                                     boolean result) {
        System.out.print("Pattern:    ");
        System.out.println("'" + pattern + "'");
        System.out.print("Expression: ");
        System.out.println("'" + expression + "'");

        boolean tested_as = fullPatternMatch(pattern, expression, '\\');
        System.out.print("Result:     ");
        System.out.print(tested_as);
        if (tested_as != result) {
            System.out.println(" *** FAILED, Expected: " + result + " ***");
        } else {
            System.out.println();
        }
        return tested_as;
    }

    public static void main(String[] args) {
        // Testing the SQL expression parser.
        testSearch("", "abc", false);
        testSearch("%", "abc", true);
        testSearch("a%", "abc", true);
        testSearch("ab%", "abc", true);
        testSearch("abc%", "abc", true);
        testSearch("abcd%", "abc", false);
        testSearch("abcd", "abc", false);
        testSearch("abc", "abc", true);
        testSearch("ab", "abc", false);
        testSearch("a", "abc", false);
        testSearch("a_", "abc", false);
        testSearch("ab_", "abc", true);
        testSearch("abc_", "abc", false);
        testSearch("a_c", "abc", true);
        testSearch("a%bc", "abc", true);
        testSearch("a%c", "abc", true);
        testSearch("%c", "abc", true);
        testSearch("a_bc", "abc", false);
        testSearch("__c", "abc", true);
        testSearch("a__", "abc", true);

        testSearch("a\\_\\_", "a__", true);
        testSearch("a\\__", "a__", true);
        testSearch("a\\__", "a_b", true);
        testSearch("\\___", "_ab", true);
        testSearch("\\_\\__", "_ab", false);
        testSearch("\\_\\__", "__b", true);

        testSearch("\\%ab", "%ab", true);
        testSearch("ab\\%", "ab%", true);
        testSearch("cab\\%", "cab", false);
        testSearch("cab%", "cab", true);

    }


    /**
     * Returns true if the given character is a wild card (unknown).
     */
    private static boolean isWildCard(char ch) {
        return (ch == ONE_CHAR || ch == ZERO_OR_MORE_CHARS);
    }

    /**
     * Matches a pattern against a string and returns true if it matches or
     * false otherwise.  This matches patterns that do not necessarily start
     * with a wild card unlike the 'patternMatch' method.
     */
    public static boolean fullPatternMatch(String pattern, final String str,
                                           char escape_char) {
        StringBuffer start = new StringBuffer();
        String rezt = null;
        int len = pattern.length();
        int i = 0;
        boolean last_escape_char = false;
        for (; i < len && rezt == null; ++i) {
            char c = pattern.charAt(i);
            if (last_escape_char) {
                last_escape_char = false;
                start.append(c);
            } else if (c == escape_char) {
                last_escape_char = true;
            } else if (isWildCard(c)) {
                rezt = pattern.substring(i);
            } else {
                start.append(c);
            }
        }

        if (rezt == null) {
            rezt = "";
        }

        String st = new String(start);

//    System.out.println("--");
//    System.out.println(str);
//    System.out.println(st);

        if (str.startsWith(st)) {
            String str_rezt = str.substring(st.length());  // (i)

            if (rezt.length() > 0) {
                return patternMatch(rezt, str_rezt, escape_char);
            } else {
                return str_rezt.length() == 0;
            }
        } else {
            return false;
        }

    }

    /**
     * This is the pattern match recurrsive method.  It recurses on each wildcard
     * expression in the pattern which makes for slightly better efficiency than
     * a character recurse algorithm.  However, patterns such as "_%_a" will
     * result in many recursive calls.
     * <p>
     * Returns true if the pattern matches.
     * <p>
     * NOTE: That "_%_" will be less efficient than "__%" and will produce the
     *   same result.
     * NOTE: It requires that a wild card character is the first character in
     *   the expression.
     * ISSUE: Pattern optimiser, we should optimise wild cards of type "%__" to
     *   "__%", or "%__%_%_%" to "____%".  Optimised forms are identical in
     *   result and more efficient.  This optimisation could be performed by the
     *   client during parsing of the LIKE statement.
     * HACKING ISSUE: Badly formed wild cards may result in hogging of server
     *   side resources.
     */
    public static boolean patternMatch(String pattern, String expression,
                                       char escape_char) {

        // Look at first character in pattern, if it's a ONE_CHAR wildcard then
        // check expression and pattern match until next wild card.

        if (pattern.charAt(0) == ONE_CHAR) {

            // Else step through each character in pattern and see if it matches up
            // with the expression until a wild card is found or the end is reached.
            // When the end of the pattern is reached, 'finished' is set to true.

            int i = 1;
            boolean finished = (i >= pattern.length() || expression.length() < 1);
            boolean last_was_escape = false;
            int checked = 0;
            while (!finished) {
                char c = pattern.charAt(i);
                if (!last_was_escape && c == escape_char) {
                    last_was_escape = true;
                    if (i >= expression.length()) {
                        return false;
                    }
                    ++i;
                } else if (last_was_escape || !isWildCard(c)) {
                    last_was_escape = false;
                    // If expression and pattern character doesn't match or end of
                    // expression reached, search has failed.
                    if (i >= expression.length() || c != expression.charAt(i)) {
                        return false;
                    }
                    ++i;
                    ++checked;
                } else {
                    // found a wildcard, so recurse on this wildcard
                    return patternMatch(pattern.substring(i), expression.substring(i),
                            escape_char);
                }

                finished = (i >= pattern.length());
            }

            // The pattern length minus any escaped characters
            int real_pattern_length = 0;
            int sz = pattern.length();
            for (int n = 0; n < sz; ++n) {
                if (pattern.charAt(n) != escape_char) {
                    ++real_pattern_length;
                } else {
                    ++n;
                }
            }

            // If pattern and expression lengths match then we have walked through
            // the expression and found a match, otherwise no match.

            return real_pattern_length == expression.length();

        }

        // Therefore we are doing a ZERO_OR_MORE_CHARS wildcard check.

        // If the pattern is '%' (ie. pattern.length() == 1 because it's only 1
        // character in length (the '%' character)) then it doesn't matter what the
        // expression is, we have found a match.

        if (pattern.length() == 1) {
            return true;
        }

        // Look at following character in pattern, and extract all the characters
        // before the next wild card.

        StringBuffer next_string = new StringBuffer();
        int i = 1;
        boolean finished = (i >= pattern.length());
        boolean last_was_escape = false;
        while (!finished) {
            char next_char = pattern.charAt(i);
            if (!last_was_escape && next_char == escape_char) {
                last_was_escape = true;
                ++i;
                if (i >= pattern.length()) {
                    finished = true;
                }
            } else if (last_was_escape || !isWildCard(next_char)) {
                last_was_escape = false;
                next_string.append(next_char);
                ++i;
                if (i >= pattern.length()) {
                    finished = true;
                }
            } else {
                finished = true;
            }
        }

        String find_string = new String(next_string);

        // Special case optimisation if we have found the end of the pattern, all
        // we need to do is check if 'find_string' is on the end of the expression.
        // eg. pattern = "%er", will have a 'find_string' of "er" and it is saying
        // 'does the expression end with 'er''.

        if (i >= pattern.length()) {
            return (expression.endsWith(find_string));
        }

        // Otherwise we must have finished with another wild card.
        // Try and find 'next_string' in the expression.  If its found then
        // recurse over the next pattern.

        int find_str_length = find_string.length();
        int str_index = expression.indexOf(find_string);

        while (str_index != -1) {

            boolean matched = patternMatch(
                    pattern.substring(1 + find_str_length),
                    expression.substring(str_index + find_str_length),
                    escape_char);

            if (matched) {
                return true;
            }

            str_index = expression.indexOf(find_string, str_index + 1);
        }

        return false;

    }

    /**
     * This is the search method.  It requires a table to search, a column of the
     * table, and a pattern.  It returns the rows in the table that match the
     * pattern if any.  Pattern searching only works successfully on columns that
     * are of type Types.DB_STRING.
     * This works by first reducing the search to all cells that contain the
     * first section of text.  ie. pattern = "Toby% ___ner" will first reduce
     * search to all rows between "Toby" and "Tobz".  This makes for better
     * efficiency.
     */
    static IntegerVector search(Table table, int column, String pattern) {
        return search(table, column, pattern, '\\');
    }

    /**
     * This is the search method.  It requires a table to search, a column of the
     * table, and a pattern.  It returns the rows in the table that match the
     * pattern if any.  Pattern searching only works successfully on columns that
     * are of type Types.DB_STRING.
     * This works by first reducing the search to all cells that contain the
     * first section of text.  ie. pattern = "Toby% ___ner" will first reduce
     * search to all rows between "Toby" and "Tobz".  This makes for better
     * efficiency.
     */
    static IntegerVector search(Table table, int column, String pattern,
                                char escape_char) {

        // Get the type for the column
        TType col_type = table.getDataTableDef().columnAt(column).getTType();

        // If the column type is not a string type then report an error.
        if (!(col_type instanceof TStringType)) {
            throw new Error("Unable to perform a pattern search " +
                    "on a non-String type column.");
        }
        TStringType col_string_type = (TStringType) col_type;

        // ---------- Pre Search ----------

        // First perform a 'pre-search' on the head of the pattern.  Note that
        // there may be no head in which case the entire column is searched which
        // has more potential to be expensive than if there is a head.

        StringBuffer pre_pattern = new StringBuffer();
        int i = 0;
        boolean finished = i >= pattern.length();
        boolean last_is_escape = false;

        while (!finished) {
            char c = pattern.charAt(i);
            if (last_is_escape) {
                last_is_escape = true;
                pre_pattern.append(c);
            } else if (c == escape_char) {
                last_is_escape = true;
            } else if (!isWildCard(c)) {
                pre_pattern.append(c);

                ++i;
                if (i >= pattern.length()) {
                    finished = true;
                }

            } else {
                finished = true;
            }
        }

        // This is set with the remaining search.
        String post_pattern;

        // This is our initial search row set.  In the second stage, rows are
        // eliminated from this vector.
        IntegerVector search_case;

        if (i >= pattern.length()) {
            // If the pattern has no 'wildcards' then just perform an EQUALS
            // operation on the column and return the results.

            TObject cell = new TObject(col_type, pattern);
            return table.selectRows(column, Operator.get("="), cell);

            // RETURN
        } else if (pre_pattern.length() == 0 ||
                col_string_type.getLocale() != null) {

            // No pre-pattern easy search :-(.  This is either because there is no
            // pre pattern (it starts with a wild-card) or the locale of the string
            // is non-lexicographical.  In either case, we need to select all from
            // the column and brute force the search space.

            search_case = table.selectAll(column);
            post_pattern = pattern;

        } else {

            // Criteria met: There is a pre_pattern, and the column locale is
            // lexicographical.

            // Great, we can do an upper and lower bound search on our pre-search
            // set.  eg. search between 'Geoff' and 'Geofg' or 'Geoff ' and
            // 'Geoff\33'

            String lower_bounds = new String(pre_pattern);
            int next_char = pre_pattern.charAt(i - 1) + 1;
            pre_pattern.setCharAt(i - 1, (char) next_char);
            String upper_bounds = new String(pre_pattern);

            post_pattern = pattern.substring(i);

            TObject cell_lower = new TObject(col_type, lower_bounds);
            TObject cell_upper = new TObject(col_type, upper_bounds);

            // Select rows between these two points.

            search_case = table.selectRows(column, cell_lower, cell_upper);

        }

        // ---------- Post search ----------

//  [This optimization assumes that (NULL like '%' = true) which is incorrect]
//    // EFFICIENCY: This is a special case efficiency case.
//    // If 'post_pattern' is '%' then we have already found all the records in
//    // our pattern.
//
//    if (post_pattern.equals("%")) {
//      return search_case;
//    }

        int pre_index = i;

        // Now eliminate from our 'search_case' any cells that don't match our
        // search pattern.
        // Note that by this point 'post_pattern' will start with a wild card.
        // This follows the specification for the 'patternMatch' method.
        // EFFICIENCY: This is a brute force iterative search.  Perhaps there is
        //   a faster way of handling this?

        BlockIntegerList i_list = new BlockIntegerList(search_case);
        IntegerIterator iterator = i_list.iterator(0, i_list.size() - 1);

        while (iterator.hasNext()) {

            // Get the expression (the contents of the cell at the given column, row)

            boolean pattern_matches = false;
            TObject cell = table.getCellContents(column, iterator.next());
            // Null values doesn't match with anything
            if (!cell.isNull()) {
                String expression = cell.getObject().toString();
                // We must remove the head of the string, which has already been
                // found from the pre-search section.
                expression = expression.substring(pre_index);
                pattern_matches = patternMatch(post_pattern, expression, escape_char);
            }
            if (!pattern_matches) {
                // If pattern does not match then remove this row from the search.
                iterator.remove();
            }

        }

        return new IntegerVector(i_list);

    }

    // ---------- Matching against a regular expression ----------

    /**
     * Matches a string against a regular expression pattern.  We use the regex
     * library as specified in the DatabaseSystem configuration.
     */
    static boolean regexMatch(TransactionSystem system,
                              String pattern, String value) {
        // If the first character is a '/' then we assume it's a Perl style regular
        // expression (eg. "/.*[0-9]+\/$/i")
        if (pattern.startsWith("/")) {
            int end = pattern.lastIndexOf('/');
            String pat = pattern.substring(1, end);
            String ops = pattern.substring(end + 1);
            return system.getRegexLibrary().regexMatch(pat, ops, value);
        } else {
            // Otherwise it's a regular expression with no operators
            return system.getRegexLibrary().regexMatch(pattern, "", value);
        }
    }

    /**
     * Matches a column of a table against a constant regular expression
     * pattern.  We use the regex library as specified in the DatabaseSystem
     * configuration.
     */
    static IntegerVector regexSearch(Table table, int column, String pattern) {
        // If the first character is a '/' then we assume it's a Perl style regular
        // expression (eg. "/.*[0-9]+\/$/i")
        if (pattern.startsWith("/")) {
            int end = pattern.lastIndexOf('/');
            String pat = pattern.substring(1, end);
            String ops = pattern.substring(end + 1);
            return table.getDatabase().getSystem().getRegexLibrary().regexSearch(
                    table, column, pat, ops);
        } else {
            // Otherwise it's a regular expression with no operators
            return table.getDatabase().getSystem().getRegexLibrary().regexSearch(
                    table, column, pattern, "");
        }
    }

}
