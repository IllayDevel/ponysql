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
 * An implementation of a QueryContext based on a DatabaseConnection object.
 *
 * @author Tobias Downer
 */

public class DatabaseQueryContext extends AbstractQueryContext {

    /**
     * The DatabaseConnection.
     */
    private DatabaseConnection database;

    /**
     * Constructs the QueryContext.
     */
    public DatabaseQueryContext(DatabaseConnection database) {
        this.database = database;
    }

    /**
     * Returns the Database object that this context is a child of.
     */
    public Database getDatabase() {
        return database.getDatabase();
    }

    /**
     * Returns a TransactionSystem object that is used to determine information
     * about the transactional system.
     */
    public TransactionSystem getSystem() {
        return getDatabase().getSystem();
    }

    /**
     * Returns the system FunctionLookup object.
     */
    public FunctionLookup getFunctionLookup() {
        return getSystem().getFunctionLookup();
    }

    /**
     * Returns the GrantManager object that is used to determine grant information
     * for the database.
     */
    public GrantManager getGrantManager() {
        return database.getGrantManager();
    }

    /**
     * Returns a DataTable from the database with the given table name.
     */
    public DataTable getTable(TableName name) {
        database.addSelectedFromTable(name);
        return database.getTable(name);
    }

    /**
     * Returns a DataTableDef for the given table name.
     */
    public DataTableDef getDataTableDef(TableName name) {
        return database.getDataTableDef(name);
    }

    /**
     * Creates a QueryPlanNode for the view with the given name.
     */
    public QueryPlanNode createViewQueryPlanNode(TableName name) {
        return database.createViewQueryPlanNode(name);
    }

    /**
     * Increments the sequence generator and returns the next unique key.
     */
    public long nextSequenceValue(String name) {
        return database.nextSequenceValue(name);
    }

    /**
     * Returns the current sequence value returned for the given sequence
     * generator within the connection defined by this context.  If a value was
     * not returned for this connection then a statement exception is generated.
     */
    public long currentSequenceValue(String name) {
        return database.lastSequenceValue(name);
    }

    /**
     * Sets the current sequence value for the given sequence generator.
     */
    public void setSequenceValue(String name, long value) {
        database.setSequenceValue(name, value);
    }

    /**
     * Returns the user name of the connection.
     */
    public String getUserName() {
        return database.getUser().getUserName();
    }

}
