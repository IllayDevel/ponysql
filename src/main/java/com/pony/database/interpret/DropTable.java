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

package com.pony.database.interpret;

import com.pony.database.*;

import java.util.ArrayList;

/**
 * The logic of the 'DROP TABLE' SQL command.
 *
 * @author Tobias Downer
 */

public class DropTable extends Statement {

    /**
     * Only create if table doesn't exist.
     */
    boolean only_if_exists = false;

    /**
     * The list of tables to drop.
     */
    ArrayList drop_tables = new ArrayList();


//  /**
//   * Adds the table name to the list of tables to drop.
//   */
//  void addTable(String table) throws ParseException {
//    if (drop_tables.contains(table)) {
//      throw new ParseException("Duplicate table in drop");
//    }
//    drop_tables.add(table);
//  }


    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {

        only_if_exists = cmd.getBoolean("only_if_exists");
        drop_tables = (ArrayList) cmd.getObject("table_list");

        // Check there are no duplicate entries in the list of tables to drop
        for (int i = 0; i < drop_tables.size(); ++i) {
            Object check = drop_tables.get(i);
            for (int n = i + 1; n < drop_tables.size(); ++n) {
                if (drop_tables.get(n).equals(check)) {
                    throw new DatabaseException("Duplicate table in drop: " + check);
                }
            }
        }

    }

    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        int list_size = drop_tables.size();
        ArrayList resolved_tables = new ArrayList(list_size);
        // Check the user has privs to delete these tables...
        for (Object drop_table : drop_tables) {
            String table_name = drop_table.toString();
            TableName tname = resolveTableName(table_name, database);
            // Does the table exist?
            if (!only_if_exists && !database.tableExists(tname)) {
                throw new DatabaseException("Table '" + tname + "' does not exist.");
            }

            resolved_tables.add(tname);
            // Does the user have privs to drop this tables?
            if (!database.getDatabase().canUserDropTableObject(context,
                    user, tname)) {
                throw new UserAccessException(
                        "User not permitted to drop table: " + tname);
            }
        }

        // Check there are no referential links to any tables being dropped
        for (int i = 0; i < list_size; ++i) {
            TableName tname = (TableName) resolved_tables.get(i);
            // Any tables that have a referential link to this table.
            Transaction.ColumnGroupReference[] refs =
                    database.queryTableImportedForeignKeyReferences(tname);
            for (Transaction.ColumnGroupReference ref : refs) {
                // If the key table isn't being dropped then error
                if (!resolved_tables.contains(ref.key_table_name)) {
                    throw new DatabaseConstraintViolationException(
                            DatabaseConstraintViolationException.DROP_TABLE_VIOLATION,
                            "Constraint violation (" + ref.name + ") dropping table " +
                                    tname + " because of referential link from " +
                                    ref.key_table_name);
                }
            }
        }


        // If the 'only if exists' flag is false, we need to check tables to drop
        // exist first.
        if (!only_if_exists) {
            // For each table to drop.
            for (int i = 0; i < list_size; ++i) {
                // Does the table already exist?
//        String table_name = drop_tables.get(i).toString();
////        TableName tname =
////               TableName.resolve(database.getCurrentSchema(), table_name);
//        TableName tname = resolveTableName(table_name, database);
                TableName tname = (TableName) resolved_tables.get(i);

                // If table doesn't exist, throw an error
                if (!database.tableExists(tname)) {
                    throw new DatabaseException("Can not drop table '" + tname +
                            "'.  It does not exist.");
                }
            }
        }

        // For each table to drop.
        int dropped_table_count = 0;
        GrantManager grant_manager = database.getGrantManager();
        for (int i = 0; i < list_size; ++i) {
            // Does the table already exist?
//      String table_name = drop_tables.get(i).toString();
//      TableName tname = resolveTableName(table_name, database);
            TableName tname = (TableName) resolved_tables.get(i);
            if (database.tableExists(tname)) {
                // Drop table in the transaction
                database.dropTable(tname);
                // Drop the grants for this object
                grant_manager.revokeAllGrantsOnObject(
                        GrantManager.TABLE, tname.toString());
                // Drop all constraints from the schema
                database.dropAllConstraintsForTable(tname);
                ++dropped_table_count;
            }
        }

        return FunctionTable.resultTable(context, 0);
    }


}
