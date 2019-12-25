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

