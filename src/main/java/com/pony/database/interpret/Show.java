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

import java.sql.SQLException;

import com.pony.database.*;
import com.pony.database.sql.ParseException;
import com.pony.database.jdbc.SQLQuery;

/**
 * Statement that handles SHOW and DESCRIBE sql commands.
 *
 * @author Tobias Downer
 */

public class Show extends Statement {

    // Various show statics,
    static final int TABLES = 1;
    static final int STATUS = 2;
    static final int DESCRIBE_TABLE = 3;
    static final int CONNECTIONS = 4;
    static final int PRODUCT = 5;
    static final int CONNECTION_INFO = 6;

    /**
     * The name the table that we are to update.
     */
    String table_name;

    /**
     * The type of information that we are to show.
     */
    String show_type;

    /**
     * Arguments of the show statement.
     */
    Expression[] args;

    /**
     * The search expression for the show statement (where clause).
     */
    SearchExpression where_clause = new SearchExpression();

    /**
     * Convenience, creates an empty table with the given column names.
     */
    TemporaryTable createEmptyTable(Database d, String name, String[] cols)
            throws DatabaseException {
        // Describe the given table...
        DataTableColumnDef[] fields = new DataTableColumnDef[cols.length];
        for (int i = 0; i < cols.length; ++i) {
            fields[i] = DataTableColumnDef.createStringColumn(cols[i]);
        }
        TemporaryTable temp_table = new TemporaryTable(d, name, fields);
        // No entries...
        temp_table.setupAllSelectableSchemes();
        return temp_table;
    }

    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {
        // Get the show variables from the query model
        show_type = (String) cmd.getObject("show");
        show_type = show_type.toLowerCase();
        table_name = (String) cmd.getObject("table_name");
        args = (Expression[]) cmd.getObject("args");
        where_clause = (SearchExpression) cmd.getObject("where_clause");
    }

    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);
        Database d = database.getDatabase();

        // Construct an executor for interpreting SQL queries inside here.
        SQLQueryExecutor executor = new SQLQueryExecutor();

        // The table we are showing,
        TemporaryTable show_table;

        try {

            // How we order the result set
            int[] order_set = null;

            if (show_type.equals("schema")) {

                SQLQuery query = new SQLQuery(
                        "  SELECT \"name\" AS \"schema_name\", " +
                                "         \"type\", " +
                                "         \"other\" AS \"notes\" " +
                                "    FROM SYS_JDBC.ThisUserSchemaInfo " +
                                "ORDER BY \"schema_name\"");
                return executor.execute(database, query);

            } else if (show_type.equals("tables")) {

                String current_schema = database.getCurrentSchema();

                SQLQuery query = new SQLQuery(
                        "  SELECT \"Tables.TABLE_NAME\" AS \"table_name\", " +
                                "         I_PRIVILEGE_STRING(\"agg_priv_bit\") AS \"user_privs\", " +
                                "         \"Tables.TABLE_TYPE\" as \"table_type\" " +
                                "    FROM SYS_JDBC.Tables, " +
                                "         ( SELECT AGGOR(\"priv_bit\") agg_priv_bit, " +
                                "                  \"object\", \"param\" " +
                                "             FROM SYS_JDBC.ThisUserSimpleGrant " +
                                "            WHERE \"object\" = 1 " +
                                "         GROUP BY \"param\" )" +
                                "   WHERE \"Tables.TABLE_SCHEM\" = ? " +
                                "     AND CONCAT(\"Tables.TABLE_SCHEM\", '.', \"Tables.TABLE_NAME\") = \"param\" " +
                                "ORDER BY \"Tables.TABLE_NAME\"");
                query.addVar(current_schema);

                return executor.execute(database, query);

            } else if (show_type.equals("status")) {

                SQLQuery query = new SQLQuery(
                        "  SELECT \"stat_name\" AS \"name\", " +
                                "         \"value\" " +
                                "    FROM SYS_INFO.DatabaseStatistics ");

                return executor.execute(database, query);

            } else if (show_type.equals("describe_table")) {

                TableName tname = resolveTableName(table_name, database);
                if (!database.tableExists(tname)) {
                    throw new StatementException(
                            "Unable to find table '" + table_name + "'");
                }

                SQLQuery query = new SQLQuery(
                        "  SELECT \"column\" AS \"name\", " +
                                "         i_sql_type(\"type_desc\", \"size\", \"scale\") AS \"type\", " +
                                "         \"not_null\", " +
                                "         \"index_str\" AS \"index\", " +
                                "         \"default\" " +
                                "    FROM SYS_JDBC.ThisUserTableColumns " +
                                "   WHERE \"schema\" = ? " +
                                "     AND \"table\" = ? " +
                                "ORDER BY \"seq_no\" ");
                query.addVar(tname.getSchema());
                query.addVar(tname.getName());

                return executor.execute(database, query);

            } else if (show_type.equals("connections")) {

                SQLQuery query = new SQLQuery(
                        "SELECT * FROM SYS_INFO.CurrentConnections");

                return executor.execute(database, query);

            } else if (show_type.equals("product")) {

                SQLQuery query = new SQLQuery(
                        "SELECT \"name\", \"version\" FROM " +
                                "  ( SELECT \"value\" AS \"name\" FROM SYS_INFO.ProductInfo " +
                                "     WHERE \"var\" = 'name' ), " +
                                "  ( SELECT \"value\" AS \"version\" FROM SYS_INFO.ProductInfo " +
                                "     WHERE \"var\" = 'version' ) "
                );

                return executor.execute(database, query);

            } else if (show_type.equals("connection_info")) {

                SQLQuery query = new SQLQuery(
                        "SELECT * FROM SYS_INFO.ConnectionInfo"
                );

                return executor.execute(database, query);

            } else if (show_type.equals("jdbc_procedures")) {
                // Need implementing?
                show_table = createEmptyTable(d, "JDBCProcedures",
                        new String[]{"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                                "R1", "R2", "R3", "REMARKS", "PROCEDURE_TYPE"});
            } else if (show_type.equals("jdbc_procedure_columns")) {
                // Need implementing?
                show_table = createEmptyTable(d, "JDBCProcedureColumns",
                        new String[]{"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                                "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE",
                                "TYPE_NAME", "PRECISION", "LENGTH", "SCALE",
                                "RADIX", "NULLABLE", "REMARKS"});
            } else if (show_type.equals("jdbc_catalogs")) {
                // Need implementing?
                show_table = createEmptyTable(d, "JDBCCatalogs",
                        new String[]{"TABLE_CAT"});
            } else if (show_type.equals("jdbc_table_types")) {
                // Describe the given table...
                DataTableColumnDef[] fields = new DataTableColumnDef[1];
                fields[0] = DataTableColumnDef.createStringColumn("TABLE_TYPE");

                TemporaryTable temp_table =
                        new TemporaryTable(d, "JDBCTableTypes", fields);
                String[] supported_types = {
                        "TABLE", "VIEW", "SYSTEM TABLE",
                        "TRIGGER", "FUNCTION", "SEQUENCE"};
                for (int i = 0; i < supported_types.length; ++i) {
                    temp_table.newRow();
                    temp_table.setRowObject(TObject.stringVal(supported_types[i]),
                            "JDBCTableTypes.TABLE_TYPE");
                }
                temp_table.setupAllSelectableSchemes();
                show_table = temp_table;
                order_set = new int[]{0};
            } else if (show_type.equals("jdbc_best_row_identifier")) {
                // Need implementing?
                show_table = createEmptyTable(d, "JDBCBestRowIdentifier",
                        new String[]{"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                                "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
                                "PSEUDO_COLUMN"});
            } else if (show_type.equals("jdbc_version_columns")) {
                // Need implementing?
                show_table = createEmptyTable(d, "JDBCVersionColumn",
                        new String[]{"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                                "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
                                "PSEUDO_COLUMN"});
            } else if (show_type.equals("jdbc_index_info")) {
                // Need implementing?
                show_table = createEmptyTable(d, "JDBCIndexInfo",
                        new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                                "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE",
                                "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                                "CARDINALITY", "PAGES", "FILTER_CONDITION"
                        });
            } else {
                throw new StatementException("Unknown SHOW identifier: " + show_type);
            }

        } catch (SQLException e) {
            throw new DatabaseException("SQL Error: " + e.getMessage());
        } catch (ParseException e) {
            throw new DatabaseException("Parse Error: " + e.getMessage());
        } catch (TransactionException e) {
            throw new DatabaseException("Transaction Error: " + e.getMessage());
        }

        return show_table;

    }


}
