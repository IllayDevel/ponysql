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

package com.pony.database.interpret;

import com.pony.database.*;

/**
 * A single table resource item in a query which handles the behaviour
 * of resolving references to columns as well as providing various base
 * utility methods for resolving general variable names.
 * <p>
 * Each instance of this interface represents a single 'FROM' resource.
 *
 * @author Tobias Downer
 */

public interface FromTableInterface {

    /**
     * Returns a unique name given to this table source.  No other sources
     * will share this identifier string.
     */
    String getUniqueName();

    /**
     * Returns true if this source will match the given catalog, schema and
     * table.  If any arguments are null then it is not included in the match.
     * <p>
     * Used for 'Part.*' type glob searches.
     */
    boolean matchesReference(String catalog, String schema, String table);

    /**
     * Returns the number of instances we can resolve the given catalog, schema,
     * table and column name to a column or columns within this item.  Note that
     * if catalog, schema, table or column is 'null' then it means it doesn't
     * matter.
     * <p>
     * For example, say we need to resolve the column 'id' the arguments are
     * null, null, null, "id".  This may resolve to multiple columns if there is
     * a mixture of tables with "id" as a column.
     * <p>
     * Note that parameters of 'null, null, null, null',
     * 'null, null, null, not null', 'null, null, not null, not null',
     * 'null, not null, not null, not null', and
     * 'not null, not null, not null, not null' are only accepted.
     */
    int resolveColumnCount(String catalog, String schema,
                           String table, String column);

    /**
     * Returns a Variable that is a fully resolved form of the given column in
     * this table set.  This method does not have to check whether the parameters
     * reference more than one column.  If more than one column is referenced,
     * the actual column returned is implementation specific.
     */
    Variable resolveColumn(String catalog, String schema,
                           String table, String column);

    /**
     * Returns an array of Variable objects that references each column
     * available in this table set item in order from left column to
     * right column.
     */
    Variable[] allColumns();

//  /**
//   * Returns a Queriable object that can be evaluated to return a tangible
//   * Table object to use in a query.
//   * <p>
//   * Note that this method would generally only be used at the end of the
//   * lifespan of this instance.
//   */
//  Queriable getQueriable();

}
