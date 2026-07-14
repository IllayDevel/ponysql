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

/**
 * An interface to an object that describes characteristics of a table based
 * object in the database.  This can represent anything that evaluates to a
 * Table when the query plan is evaluated.  It is used to represent data tables
 * and views.
 * <p>
 * This object is used by the planner to see ahead of time what sort of table
 * we are dealing with.  For example, a view is stored with a DataTableDef
 * describing the resultant columns, and the QueryPlanNode to produce the
 * view result.  The query planner requires the information in DataTableDef
 * to resolve references in the query, and the QueryPlanNode to add into the
 * resultant plan tree.
 *
 * @author Tobias Downer
 */

public interface TableQueryDef {

    /**
     * Returns an immutable DataTableDef object that describes the columns in this
     * table source, and the name of the table.
     */
    DataTableDef getDataTableDef();

    /**
     * Returns a QueryPlanNode that can be put into a plan tree and can be
     * evaluated to find the result of the table.  This method should always
     * return a new object representing the query plan.
     */
    QueryPlanNode getQueryPlanNode();

}

