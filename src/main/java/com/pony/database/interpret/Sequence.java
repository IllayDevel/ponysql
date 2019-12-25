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
 * A statement tree for creating and dropping sequence generators.
 *
 * @author Tobias Downer
 */

public class Sequence extends Statement {

    String type;

    TableName seq_name;

    Expression increment;
    Expression min_value;
    Expression max_value;
    Expression start_value;
    Expression cache_value;
    boolean cycle;

    // ----------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {
        type = (String) cmd.getObject("type");
        String sname = (String) cmd.getObject("seq_name");
        String schema_name = database.getCurrentSchema();
        seq_name = TableName.resolve(schema_name, sname);
        seq_name = database.tryResolveCase(seq_name);

        if (type.equals("create")) {
            // Resolve the function name into a TableName object.
            increment = (Expression) cmd.getObject("increment");
            min_value = (Expression) cmd.getObject("min_value");
            max_value = (Expression) cmd.getObject("max_value");
            start_value = (Expression) cmd.getObject("start");
            cache_value = (Expression) cmd.getObject("cache");
            cycle = cmd.getObject("cycle") != null;
        }

    }

    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        // Does the schema exist?
        boolean ignore_case = database.isInCaseInsensitiveMode();
        SchemaDef schema =
                database.resolveSchemaCase(seq_name.getSchema(), ignore_case);
        if (schema == null) {
            throw new DatabaseException("Schema '" + seq_name.getSchema() +
                    "' doesn't exist.");
        } else {
            seq_name = new TableName(schema.getName(), seq_name.getName());
        }

        if (type.equals("create")) {

            // Does the user have privs to create this sequence generator?
            if (!database.getDatabase().canUserCreateSequenceObject(context,
                    user, seq_name)) {
                throw new UserAccessException(
                        "User not permitted to create sequence: " + seq_name);
            }

            // Does a table already exist with this name?
            if (database.tableExists(seq_name)) {
                throw new DatabaseException("Database object with name '" + seq_name +
                        "' already exists.");
            }

            // Resolve the expressions,
            long v_start_value = 0;
            if (start_value != null) {
                v_start_value =
                        start_value.evaluate(null, null, context).toBigNumber().longValue();
            }
            long v_increment_by = 1;
            if (increment != null) {
                v_increment_by =
                        increment.evaluate(null, null, context).toBigNumber().longValue();
            }
            long v_min_value = 0;
            if (min_value != null) {
                v_min_value =
                        min_value.evaluate(null, null, context).toBigNumber().longValue();
            }
            long v_max_value = Long.MAX_VALUE;
            if (max_value != null) {
                v_max_value =
                        max_value.evaluate(null, null, context).toBigNumber().longValue();
            }
            long v_cache = 16;
            if (cache_value != null) {
                v_cache =
                        cache_value.evaluate(null, null, context).toBigNumber().longValue();
                if (v_cache <= 0) {
                    throw new DatabaseException("Cache size can not be <= 0");
                }
            }

            if (v_min_value >= v_max_value) {
                throw new DatabaseException("Min value can not be >= the max value.");
            }
            if (v_start_value < v_min_value ||
                    v_start_value >= v_max_value) {
                throw new DatabaseException(
                        "Start value is outside the min/max sequence bounds.");
            }

            database.createSequenceGenerator(seq_name,
                    v_start_value, v_increment_by, v_min_value, v_max_value,
                    v_cache, cycle);

            // The initial grants for a sequence is to give the user who created it
            // full access.
            database.getGrantManager().addGrant(
                    Privileges.PROCEDURE_ALL_PRIVS, GrantManager.TABLE,
                    seq_name.toString(), user.getUserName(), true,
                    Database.INTERNAL_SECURE_USERNAME);

        } else if (type.equals("drop")) {

            // Does the user have privs to create this sequence generator?
            if (!database.getDatabase().canUserDropSequenceObject(context,
                    user, seq_name)) {
                throw new UserAccessException(
                        "User not permitted to drop sequence: " + seq_name);
            }

            database.dropSequenceGenerator(seq_name);

            // Drop the grants for this object
            database.getGrantManager().revokeAllGrantsOnObject(
                    GrantManager.TABLE, seq_name.toString());

        } else {
            throw new RuntimeException("Unknown type: " + type);
        }

        // Return an update result table.
        return FunctionTable.resultTable(context, 0);

    }

}

