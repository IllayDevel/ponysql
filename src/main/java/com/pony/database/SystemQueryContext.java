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
 * A QueryContext that only wraps around a TransactionSystem and does not
 * provide implementations for the 'getTable', and 'getDatabase' methods.
 *
 * @author Tobias Downer
 */

final class SystemQueryContext extends AbstractQueryContext {

    /**
     * The wrapped TransactionSystem object.
     */
    private final TransactionSystem system;

    /**
     * The Transaction this is a part of.
     */
    private final SimpleTransaction transaction;

    /**
     * The context schema of this context.
     */
    private final String current_schema;


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
