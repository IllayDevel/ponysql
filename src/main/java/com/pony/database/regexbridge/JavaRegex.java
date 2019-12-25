/*
 * Pony SQL Database ( http://www.ponysql.ru/ )
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

package com.pony.database.regexbridge;

import com.pony.database.Table;
import com.pony.database.TObject;
import com.pony.util.IntegerVector;

import java.util.regex.*;

/**
 * A bridge to the internal Java regular expression library that was introduced
 * in Java 1.4.  This bridge will only work if the regular expression API
 * is available in the class library.  It is not available in 1.3 and 1.2.
 *
 * @author Tobias Downer
 */

public class JavaRegex implements com.pony.database.RegexLibrary {

    public boolean regexMatch(String regular_expression, String expression_ops,
                              String value) {
        try {
            // PENDING: Compile and cache most commonly used regular expressions...

            int c_flags = 0;
            if (expression_ops != null) {
                if (expression_ops.indexOf('i') != -1) {
                    c_flags += Pattern.CASE_INSENSITIVE;
                }
                if (expression_ops.indexOf('s') != -1) {
                    c_flags += Pattern.DOTALL;
                }
                if (expression_ops.indexOf('m') != -1) {
                    c_flags += Pattern.MULTILINE;
                }
            }

            Pattern pattern = Pattern.compile(regular_expression, c_flags);
            Matcher matcher = pattern.matcher(value);
            return matcher.matches();
        } catch (PatternSyntaxException e) {
            // Incorrect syntax means we always match to false,
            return false;
        }
    }

    public IntegerVector regexSearch(Table table, int column,
                                     String regular_expression, String expression_ops) {
        // Get the ordered column,
        IntegerVector row_list = table.selectAll(column);
        // The result matched rows,
        IntegerVector result_list = new IntegerVector();

        // Make into a new list that matches the pattern,
        Pattern pattern;
        try {
            // PENDING: Compile and cache most commonly used regular expressions...

            int c_flags = 0;
            if (expression_ops != null) {
                if (expression_ops.indexOf('i') != -1) {
                    c_flags += Pattern.CASE_INSENSITIVE;
                }
                if (expression_ops.indexOf('s') != -1) {
                    c_flags += Pattern.DOTALL;
                }
                if (expression_ops.indexOf('m') != -1) {
                    c_flags += Pattern.MULTILINE;
                }
            }

            pattern = Pattern.compile(regular_expression, c_flags);
        } catch (PatternSyntaxException e) {
            // Incorrect syntax means we always match to an empty list,
            return result_list;
        }

        // For each row in the column, test it against the regular expression.
        int size = row_list.size();
        for (int i = 0; i < size; ++i) {
            int row_index = row_list.intAt(i);
            TObject cell = table.getCellContents(column, row_index);
            // Only try and match against non-null cells.
            if (!cell.isNull()) {
                Object ob = cell.getObject();
                String str = ob.toString();
                // If the column matches the regular expression then return it,
                if (pattern.matcher(str).matches()) {
                    result_list.addInt(row_index);
                }
            }
        }

        return result_list;
    }

}
