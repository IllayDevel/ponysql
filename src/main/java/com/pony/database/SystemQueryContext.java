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
 * A QueryContext that only wraps around a TransactionSystem and does not
 * provide implementations for the 'getTable', and 'getDatabase' methods.
 *
 * @author Tobias Downer
 */

final class SystemQueryContext extends AbstractQueryContext {

    /**
     * The wrapped TransactionSystem object.
     */
    private TransactionSystem system;

    /**
     * The Transaction this is a part of.
     */
    private SimpleTransaction transaction;

    /**
     * The context schema of this context.
     */
    private String current_schema;


    /**
     * Constructs the QueryContext.
     */
    SystemQueryContext(SimpleTransaction transaction,
                       String current_schema) {
        this.transaction = transaction;
        this.system = transaction.getSystem();
        this.current_schema = current_schema;
    }

    /**
     * Returns a TransactionSystem object that is used to determine information
     * about the transactional system.
     */
    public TransactionSystem getSystem() {
        return system;
    }

    /**
     * Returns the system FunctionLookup object.
     */
    public FunctionLookup getFunctionLookup() {
        return getSystem().getFunctionLookup();
    }

    /**
     * Increments the sequence generator and returns the next unique key.
     */
    public long nextSequenceValue(String name) {
        TableName tn = transaction.resolveToTableName(current_schema, name,
                system.ignoreIdentifierCase());
        return transaction.nextSequenceValue(tn);
    }

    /**
     * Returns the current sequence value returned for the given sequence
     * generator within the connection defined by this context.  If a value was
     * not returned for this connection then a statement exception is generated.
     */
    public long currentSequenceValue(String name) {
        TableName tn = transaction.resolveToTableName(current_schema, name,
                system.ignoreIdentifierCase());
        return transaction.lastSequenceValue(tn);
    }

    /**
     * Sets the current sequence value for the given sequence generator.
     */
    public void setSequenceValue(String name, long value) {
        TableName tn = transaction.resolveToTableName(current_schema, name,
                system.ignoreIdentifierCase());
        transaction.setSequenceValue(tn, value);
    }

    /**
     * Returns a unique key for the given table source in the database.
     */
    public long nextUniqueID(String table_name) {
        TableName tname = TableName.resolve(current_schema, table_name);
        return transaction.nextUniqueID(tname);
    }

    /**
     * Returns the user name of the connection.
     */
    public String getUserName() {
        return "@SYSTEM";
    }

}
