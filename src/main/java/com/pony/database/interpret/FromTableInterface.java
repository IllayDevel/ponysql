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
