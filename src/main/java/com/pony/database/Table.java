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

package com.pony.database;

import com.pony.util.IntegerVector;
import com.pony.debug.*;

//import com.pony.database.sql.SelectStatement;    // Evaluating sub-selects
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.io.PrintStream;

/**
 * This is a definition for a table in the database.  It stores the name of
 * the table, and the fields (columns) in the table.  A table represents either
 * a 'core' DataTable that directly maps to the information stored in the
 * database, or a temporary table generated on the fly.
 * <p>
 * It is an abstract class, because it does not implement the methods to add,
 * remove or access row data in the table.
 *
 * @author Tobias Downer
 */

public abstract class Table implements TableDataSource {

    // Set to true to output query debugging information.  All table operation
    // commands will be output.
    protected static final boolean DEBUG_QUERY = true;


    /**
     * The Constructor.  Requires a name and the fields in the table.
     */
    protected Table() {
    }

    /**
     * Returns the Database object that this table is derived from.
     */
    public abstract Database getDatabase();

    /**
     * Returns the TransactionSystem object that this table is part of.
     */
    public final TransactionSystem getSystem() {
        return getDatabase().getSystem();
    }

    /**
     * Returns a DebugLogger object that we can use to log debug messages to.
     */
    public DebugLogger Debug() {
        return getSystem().Debug();
    }

    /**
     * Returns the number of columns in the table.
     */
    public abstract int getColumnCount();

    /**
     * Returns the number of rows stored in the table.
     */
    public abstract int getRowCount();

    /**
     * Returns a TType object that would represent values at the given
     * column index.  Throws an error if the column can't be found.
     */
    public TType getTTypeForColumn(int column) {
        return getDataTableDef().columnAt(column).getTType();
    }

    /**
     * Returns a TType object that would represent values in the given
     * column.  Throws an error if the column can't be found.
     */
    public TType getTTypeForColumn(Variable v) {
        return getTTypeForColumn(findFieldName(v));
    }

    /**
     * Given a fully qualified variable field name, ie. 'APP.CUSTOMER.CUSTOMERID'
     * this will return the column number the field is at.  Returns -1 if the
     * field does not exist in the table.
     */
    public abstract int findFieldName(Variable v);

    /**
     * Returns a fully qualified Variable object that represents the name of
     * the column at the given index.  For example,
     *   new Variable(new TableName("APP", "CUSTOMER"), "ID")
     */
    public abstract Variable getResolvedVariable(int column);

    /**
     * Returns a SelectableScheme for the given column in the given VirtualTable
     * row domain.  The 'column' variable may be modified as it traverses through
     * the tables, however the 'original_column' retains the link to the column
     * in 'table'.
     */
    abstract SelectableScheme getSelectableSchemeFor(int column, int original_column, Table table);

    /**
     * Given a set, this trickles down through the Table hierarchy resolving
     * the given row_set to a form that the given ancestor understands.
     * Say you give the set { 0, 1, 2, 3, 4, 5, 6 }, this function may check
     * down three levels and return a new 7 element set with the rows fully
     * resolved to the given ancestors domain.
     */
    abstract void setToRowTableDomain(int column, IntegerVector row_set,
                                      TableDataSource ancestor);

    /**
     * Return the list of DataTable and row sets that make up the raw information
     * in this table.
     */
    abstract RawTableInformation resolveToRawTable(RawTableInformation info);

    /**
     * Returns an object that represents the information in the given cell
     * in the table.  This will generally be an expensive algorithm, so calls
     * to it should be kept to a minimum.  Note that the offset between two
     * rows is not necessarily 1.  Use 'rowEnumeration' to get the contents
     * of a set.
     */
    public abstract TObject getCellContents(int column, int row);

    /**
     * Returns an Enumeration of the rows in this table.  Each call to
     * 'RowEnumeration.nextRowIndex()' returns the next valid row in the table.
     * Note that the order that rows are retreived depend on a number of factors.
     * For a DataTable the rows are accessed in the order they are in the data
     * file.  For a VirtualTable, the rows are accessed in the order of the last
     * select operation.
     * <p>
     * If you want the rows to be returned by a specific column order then use
     * the 'selectxxx' methods.
     */
    public abstract RowEnumeration rowEnumeration();

    /**
     * Returns a DataTableDef object that defines the name of the table and the
     * layout of the columns of the table.  Note that for tables that are joined
     * with other tables, the table name and schema for this object become
     * mangled.  For example, a table called 'PERSON' joined with a table called
     * 'MUSIC' becomes a table called 'PERSON#MUSIC' in a null schema.
     */
    public abstract DataTableDef getDataTableDef();

    /**
     * Adds a DataTableListener to the DataTable objects at the root of this
     * table tree hierarchy.  If this table represents the join of a number of
     * tables then the DataTableListener is added to all the DataTable objects
     * at the root.
     * <p>
     * A DataTableListener is notified of all modifications to the raw entries
     * of the table.  This listener can be used for detecting changes in VIEWs,
     * for triggers or for caching of common queries.
     */
    abstract void addDataTableListener(DataTableListener listener);

    /**
     * Removes a DataTableListener from the DataTable objects at the root of
     * this table tree hierarchy.  If this table represents the join of a
     * number of tables, then the DataTableListener is removed from all the
     * DataTable objects at the root.
     */
    abstract void removeDataTableListener(DataTableListener listener);

    /**
     * Locks the root table(s) of this table so that it is impossible to
     * overwrite the underlying rows that may appear in this table.
     * This is used when cells in the table need to be accessed 'outside' the
     * lock.  So we may have late access to cells in the table.
     * 'lock_key' is a given key that will also unlock the root table(s).
     * NOTE: This is nothing to do with the 'LockingMechanism' object.
     */
    public abstract void lockRoot(int lock_key);

    /**
     * Unlocks the root tables so that the underlying rows may
     * once again be used if they are not locked and have been removed.  This
     * should be called some time after the rows have been locked.
     */
    public abstract void unlockRoot(int lock_key);

    /**
     * Returns true if the table has its row roots locked (via the lockRoot(int)
     * method.
     */
    public abstract boolean hasRootsLocked();

    // ---------- Implemented from TableDataSource ----------

    /**
     * Returns the SelectableScheme that indexes the given column in this table.
     */
    public SelectableScheme getColumnScheme(int column) {
        return getSelectableSchemeFor(column, column, this);
    }


    // ---------- Convenience methods ----------

    /**
     * Returns the DataTableColumnDef object for the given column index.
     */
    public DataTableColumnDef getColumnDefAt(int col_index) {
        return getDataTableDef().columnAt(col_index);
    }


    /**
     * Dumps the contents of the table in a human readable form to the given
     * output stream.
     * This should only be used for debuging the database.
     */
    public final void dumpTo(PrintStream out) throws IOException {
        DumpHelper.dump(this, out);
    }

    /**
     * Returns a new Table based on this table with no rows in it.
     */
    public final Table emptySelect() {
        if (getRowCount() == 0) {
            return this;
        } else {
            VirtualTable table = new VirtualTable(this);
            table.set(this, new IntegerVector(0));
            return table;
        }
    }

    /**
     * Selects a single row at the given index from this table.
     */
    public final Table singleRowSelect(int row_index) {
        VirtualTable table = new VirtualTable(this);
        IntegerVector ivec = new IntegerVector(1);
        ivec.addInt(row_index);
        table.set(this, ivec);
        return table;
    }

    /**
     * Returns a Table that is a merge of this table and the destination table.
     * The rows that are in the destination table are included in this table.
     * The tables must have
     */
    public final Table columnMerge(Table table) {
        if (getRowCount() != table.getRowCount()) {
            throw new Error("Tables have different row counts.");
        }
        // Create the new VirtualTable with the joined tables.

        IntegerVector all_row_set = new IntegerVector();
        int rcount = getRowCount();
        for (int i = 0; i < rcount; ++i) {
            all_row_set.addInt(i);
        }

        Table[] tabs = new Table[]{this, table};
        IntegerVector[] row_sets = new IntegerVector[]
                {all_row_set, all_row_set};

        VirtualTable out_table = new VirtualTable(tabs);
        out_table.set(tabs, row_sets);

        return out_table;
    }


    // ---------- Queries using Expression class ----------

    /**
     * A single column range select on this table.  This can often be solved
     * very quickly especially if there is an index on the column.  The
     * SelectableRange array represents a set of ranges that are returned that
     * meet the given criteria.
     *
     * @param col_var the column variable in this table (eg. Part.id)
     * @param ranges the normalized (no overlapping) set of ranges to find.
     */
    public final Table rangeSelect(Variable col_var, SelectableRange[] ranges) {
        // If this table is empty then there is no range to select so
        // trivially return this object.
        if (getRowCount() == 0) {
            return this;
        }
        // Are we selecting a black or null range?
        if (ranges == null || ranges.length == 0) {
            // Yes, so return an empty table
            return emptySelect();
        }
        // Are we selecting the entire range?
        if (ranges.length == 1 &&
                ranges[0].equals(SelectableRange.FULL_RANGE)) {
            // Yes, so return this table.
            return this;
        }

        // Must be a non-trivial range selection.

        // Find the column index of the column selected
        int column = findFieldName(col_var);

        if (column == -1) {
            throw new RuntimeException(
                    "Unable to find the column given to select the range of: " +
                            col_var.getName());
        }

        // Select the range
        IntegerVector rows;
        rows = selectRange(column, ranges);

        // Make a new table with the range selected
        VirtualTable table = new VirtualTable(this);
        table.set(this, rows);

        // We know the new set is ordered by the column.
        table.optimisedPostSet(column);

        if (DEBUG_QUERY) {
            if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                Debug().write(Lvl.INFORMATION, this,
                        table + " = " + this + ".rangeSelect(" +
                                col_var + ", " + Arrays.toString(ranges) + " )");
            }
        }

        return table;

    }

    /**
     * A simple select on this table.  We select against a column, with an
     * Operator and a rhs Expression that is constant (only needs to be
     * evaluated once).
     *
     * @param context the context of the query.
     * @param lhs_var the left has side column reference.
     * @param op the operator.
     * @param rhs the expression to select against (the expression <b>must</b>
     *   be a constant).
     */
    public final Table simpleSelect(QueryContext context, Variable lhs_var,
                                    Operator op, Expression rhs) {

        String DEBUG_SELECT_WITH = null;

        // Find the row with the name given in the condition.
        int column = findFieldName(lhs_var);

        if (column == -1) {
            throw new RuntimeException(
                    "Unable to find the LHS column specified in the condition: " +
                            lhs_var.getName());
        }

        IntegerVector rows;

        boolean ordered_by_select_column = false;

        // If we are doing a sub-query search
        if (op.isSubQuery()) {

            // We can only handle constant expressions in the RHS expression, and
            // we must assume that the RHS is a Expression[] array.
            Object ob = rhs.last();
            if (!(ob instanceof TObject)) {
                throw new RuntimeException("Sub-query not a TObject");
            }
            TObject tob = (TObject) ob;
            if (tob.getTType() instanceof TArrayType) {
                Expression[] list = (Expression[]) tob.getObject();

                // Construct a temporary table with a single column that we are
                // comparing to.
                TemporaryTable table;
                DataTableColumnDef col = getColumnDefAt(findFieldName(lhs_var));
                DatabaseQueryContext db_context = (DatabaseQueryContext) context;
                table = new TemporaryTable(db_context.getDatabase(),
                        "single", new DataTableColumnDef[]{col});

                for (Expression expression : list) {
                    table.newRow();
                    table.setRowObject(expression.evaluate(null, null, context), 0);
                }
                table.setupAllSelectableSchemes();

                // Perform the any/all sub-query on the constant table.

                return TableFunctions.anyAllNonCorrelated(
                        this, new Variable[]{lhs_var}, op, table);

            } else {
                throw new RuntimeException("Error with format or RHS expression.");
            }

        }
        // If we are doing a LIKE or REGEX pattern search
        else if (op.is("like") || op.is("not like") || op.is("regex")) {

            // Evaluate the right hand side.  We know rhs is constant so don't
            // bother passing a VariableResolver object.
            TObject rhs_const = rhs.evaluate(null, context);

            if (op.is("regex")) {
                // Use the regular expression search to determine matching rows.
                rows = selectFromRegex(column, op, rhs_const);
            } else {
                // Use the standard SQL pattern matching engine to determine
                // matching rows.
                rows = selectFromPattern(column, op, rhs_const);
            }
            // These searches guarentee result is ordered by the column
            ordered_by_select_column = true;

            // Describe the 'LIKE' select
            if (DEBUG_QUERY) {
                DEBUG_SELECT_WITH = op.toString() + " " + rhs_const;
            }

        }
        // Otherwise, we doing an index based comparison.
        else {

            // Is the column we are searching on indexable?
            DataTableColumnDef col_def = getColumnDefAt(column);
            if (!col_def.isIndexableType()) {
                throw new StatementException("Can not search on field type " +
                        col_def.getSQLTypeString() +
                        " in '" + col_def.getName() + "'");
            }

            // Evaluate the right hand side.  We know rhs is constant so don't
            // bother passing a VariableResolver object.
            TObject rhs_const = rhs.evaluate(null, context);

            // Get the rows of the selected set that match the given condition.
            rows = selectRows(column, op, rhs_const);
            ordered_by_select_column = true;

            // Describe the select
            if (DEBUG_QUERY) {
                DEBUG_SELECT_WITH = op.toString() + " " + rhs_const;
            }

        }

        // We now has a set of rows from this table to make into a
        // new table.

        VirtualTable table = new VirtualTable(this);
        table.set(this, rows);

        // OPTIMIZATION: Since we know that the 'select' return is ordered by the
        //   LHS column, we can easily generate a SelectableScheme for the given
        //   column.  This doesn't work for the non-constant set.

        if (ordered_by_select_column) {
            table.optimisedPostSet(column);
        }

        if (DEBUG_QUERY) {
            if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                Debug().write(Lvl.INFORMATION, this,
                        table + " = " + this + ".simpleSelect(" +
                                lhs_var + " " + DEBUG_SELECT_WITH + " )");
            }
        }

        return table;

    }

    /**
     * A simple join operation.  A simple join operation is one that has a
     * single joining operator, a Variable on the lhs and a simple expression on
     * the rhs that includes only columns in the rhs table. For example,
     * 'id = part_id' or 'id == part_id * 2' or 'id == part_id + vendor_id * 2'
     * <p>
     * It is important to understand how this algorithm works because all
     * optimization of the expression must happen before the method starts.
     * <p>
     * The simple join algorithm works as follows:  Every row of the right hand
     * side table 'table' is iterated through.  The select opreation is applied
     * to this table given the result evaluation.  Each row that matches is
     * included in the result table.
     * <p>
     * For optimal performance, the expression should be arranged so that the rhs
     * table is the smallest of the two tables (because we must iterate through
     * all rows of this table).  This table should be the largest.
     */
    public final Table simpleJoin(QueryContext context, Table table,
                                  Variable lhs_var, Operator op, Expression rhs) {

        // Find the row with the name given in the condition.
        int lhs_column = findFieldName(lhs_var);

        if (lhs_column == -1) {
            throw new RuntimeException(
                    "Unable to find the LHS column specified in the condition: " +
                            lhs_var.toString());
        }

        // Create a variable resolver that can resolve columns in the destination
        // table.
        TableVariableResolver resolver = table.getVariableResolver();

        // The join algorithm.  It steps through the RHS expression, selecting the
        // cells that match the relation from the LHS table (this table).

        IntegerVector this_row_set = new IntegerVector();
        IntegerVector table_row_set = new IntegerVector();

        RowEnumeration e = table.rowEnumeration();

        while (e.hasMoreRows()) {
            int row_index = e.nextRowIndex();
            resolver.setRow(row_index);

            // Resolve expression into a constant.
            TObject rhs_val = rhs.evaluate(resolver, context);

            // Select all the rows in this table that match the joining condition.
            IntegerVector selected_set = selectRows(lhs_column, op, rhs_val);

            // Include in the set.
            int size = selected_set.size();
            for (int i = 0; i < size; ++i) {
                table_row_set.addInt(row_index);
            }
            this_row_set.append(selected_set);

        }

        // Create the new VirtualTable with the joined tables.

        Table[] tabs = new Table[]{this, table};
        IntegerVector[] row_sets = new IntegerVector[]
                {this_row_set, table_row_set};

        VirtualTable out_table = new VirtualTable(tabs);
        out_table.set(tabs, row_sets);

        if (DEBUG_QUERY) {
            if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                Debug().write(Lvl.INFORMATION, this,
                        out_table + " = " + this + ".simpleJoin(" + table +
                                ", " + lhs_var + ", " + op + ", " + rhs + " )");
            }
        }

        return out_table;

    }

    /**
     * Exhaustively searches through this table for rows that match the
     * expression given.  This is the slowest type of query and is not able to
     * use any type of indexing.
     * <p>
     * A QueryContext object is used for resolving sub-query plans.  If there
     * are no sub-query plans in the expression, this can safely be 'null'.
     */
    public final Table exhaustiveSelect(QueryContext context, Expression exp) {

        Table result = this;

        // Exit early if there's nothing in the table to select from
        int row_count = getRowCount();
        if (row_count > 0) {

            TableVariableResolver resolver = getVariableResolver();
            RowEnumeration e = rowEnumeration();

            IntegerVector selected_set = new IntegerVector(row_count);

            while (e.hasMoreRows()) {
                int row_index = e.nextRowIndex();
                resolver.setRow(row_index);

                // Resolve expression into a constant.
                TObject rhs_val = exp.evaluate(resolver, context);

                // If resolved to true then include in the selected set.
                if (!rhs_val.isNull() && rhs_val.getTType() instanceof TBooleanType &&
                        rhs_val.getObject().equals(Boolean.TRUE)) {
                    selected_set.addInt(row_index);
                }

            }

            // Make into a table to return.
            VirtualTable table = new VirtualTable(this);
            table.set(this, selected_set);

            result = table;
        }

        if (DEBUG_QUERY) {
            if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                Debug().write(Lvl.INFORMATION, this,
                        result + " = " + this + ".exhaustiveSelect(" +
                                exp + " )");
            }
        }

        return result;
    }

    /**
     * Evaluates a non-correlated ANY type operator given the LHS expression,
     * the RHS subquery and the ANY operator to use.  For example;
     * <p><pre>
     *   Table.col > ANY ( SELECT .... )
     * </pre><p>
     * ANY creates a new table that contains only the rows in this table that
     * the expression and operator evaluate to true for any values in the
     * given table.
     * <p>
     * The IN operator can be represented by using '= ANY'.
     * <p>
     * Note that unlike the other join and select methods in this object this
     * will take a complex expression as the lhs provided all the Variable
     * objects resolve to this table.
     *
     * @param lhs the left has side expression.  The Variable objects in this
     *   expression must all reference columns in this table.
     * @param op the operator to use.
     * @param right_table the subquery table should only contain on column.
     * @param context the context of the query.
     */
    public Table any(QueryContext context, Expression lhs, Operator op,
                     Table right_table) {
        Table table = right_table;

        // Check the table only has 1 column
        if (table.getColumnCount() != 1) {
            throw new Error("Input table <> 1 columns.");
        }

        // Handle trivial case of no entries to select from
        if (getRowCount() == 0) {
            return this;
        }
        // If 'table' is empty then we return an empty set.  ANY { empty set } is
        // always false.
        if (table.getRowCount() == 0) {
            return emptySelect();
        }

        // Is the lhs expression a constant?
        if (lhs.isConstant()) {
            // We know lhs is a constant so no point passing arguments,
            TObject lhs_const = lhs.evaluate(null, context);
            // Select from the table.
            IntegerVector ivec = table.selectRows(0, op, lhs_const);
            if (ivec.size() > 0) {
                // There's some entries so return the whole table,
                return this;
            }
            // No entries matches so return an empty table.
            return emptySelect();
        }

        Table source_table;
        int lhs_col_index;
        // Is the lhs expression a single variable?
        Variable lhs_var = lhs.getVariable();
        // NOTE: It'll be less common for this part to be called.
        if (lhs_var == null) {
            // This is a complex expression so make a FunctionTable as our new
            // source.
            DatabaseQueryContext db_context = (DatabaseQueryContext) context;
            FunctionTable fun_table = new FunctionTable(
                    this, new Expression[]{lhs}, new String[]{"1"}, db_context);
            source_table = fun_table;
            lhs_col_index = 0;
        } else {
            // The expression is an easy to resolve reference in this table.
            source_table = this;
            lhs_col_index = source_table.findFieldName(lhs_var);
            if (lhs_col_index == -1) {
                throw new Error("Can't find column '" + lhs_var + "'.");
            }
        }

        // Check that the first column of 'table' is of a compatible type with
        // source table column (lhs_col_index).
        // ISSUE: Should we convert to the correct type via a FunctionTable?
        DataTableColumnDef source_col = source_table.getColumnDefAt(lhs_col_index);
        DataTableColumnDef dest_col = table.getColumnDefAt(0);
        if (!source_col.getTType().comparableTypes(dest_col.getTType())) {
            throw new Error("The type of the sub-query expression " +
                    source_col.getSQLTypeString() + " is incompatible " +
                    "with the sub-query " + dest_col.getSQLTypeString() +
                    ".");
        }

        // We now have all the information to solve this query.
        // We work out as follows:
        //   For >, >= type ANY we find the lowest value in 'table' and
        //   select from 'source' all the rows that are >, >= than the
        //   lowest value.
        //   For <, <= type ANY we find the highest value in 'table' and
        //   select from 'source' all the rows that are <, <= than the
        //   highest value.
        //   For = type ANY we use same method from INHelper.
        //   For <> type ANY we iterate through 'source' only including those
        //   rows that a <> query on 'table' returns size() != 0.

        IntegerVector select_vec;
        if (op.is(">") || op.is(">=")) {
            // Select the first from the set (the lowest value),
            TObject lowest_cell = table.getFirstCellContent(0);
            // Select from the source table all rows that are > or >= to the
            // lowest cell,
            select_vec = source_table.selectRows(lhs_col_index, op, lowest_cell);
        } else if (op.is("<") || op.is("<=")) {
            // Select the last from the set (the highest value),
            TObject highest_cell = table.getLastCellContent(0);
            // Select from the source table all rows that are < or <= to the
            // highest cell,
            select_vec = source_table.selectRows(lhs_col_index, op, highest_cell);
        } else if (op.is("=")) {
            // Equiv. to IN
            select_vec = INHelper.in(source_table, table, lhs_col_index, 0);
        } else if (op.is("<>")) {
            // Select the value that is the same of the entire column
            TObject cell = table.getSingleCellContent(0);
            if (cell != null) {
                // All values from 'source_table' that are <> than the given cell.
                select_vec = source_table.selectRows(lhs_col_index, op, cell);
            } else {
                // No, this means there are different values in the given set so the
                // query evaluates to the entire table.
                return this;
            }
        } else {
            throw new Error("Don't understand operator '" + op + "' in ANY.");
        }

        // Make into a table to return.
        VirtualTable rtable = new VirtualTable(this);
        rtable.set(this, select_vec);

        // Query logging information
        if (Debug().isInterestedIn(Lvl.INFORMATION)) {
            Debug().write(Lvl.INFORMATION, this,
                    rtable + " = " + this + ".any(" +
                            lhs + ", " + op + ", " + right_table + ")");
        }

        return rtable;
    }

    /**
     * Evaluates a non-correlated ALL type operator given the LHS expression,
     * the RHS subquery and the ALL operator to use.  For example;
     * <p><pre>
     *   Table.col > ALL ( SELECT .... )
     * </pre><p>
     * ALL creates a new table that contains only the rows in this table that
     * the expression and operator evaluate to true for all values in the
     * giventable.
     * <p>
     * The NOT IN operator can be represented by using '<> ALL'.
     * <p>
     * Note that unlike the other join and select methods in this object this
     * will take a complex expression as the lhs provided all the Variable
     * objects resolve to this table.
     *
     * @param lhs the left has side expression.  The Variable objects in this
     *   expression must all reference columns in this table.
     * @param op the operator to use.
     * @param table The subquery table should only contain on column.
     * @param context The context of the query.
     */
    public Table all(QueryContext context, Expression lhs, Operator op,
                     Table table) {

        // Check the table only has 1 column
        if (table.getColumnCount() != 1) {
            throw new Error("Input table <> 1 columns.");
        }

        // Handle trivial case of no entries to select from
        if (getRowCount() == 0) {
            return this;
        }
        // If 'table' is empty then we return the complete set.  ALL { empty set }
        // is always true.
        if (table.getRowCount() == 0) {
            return this;
        }

        // Is the lhs expression a constant?
        if (lhs.isConstant()) {
            // We know lhs is a constant so no point passing arguments,
            TObject lhs_const = lhs.evaluate(null, context);
            boolean compared_to_true;
            // The various operators
            if (op.is(">") || op.is(">=")) {
                // Find the maximum value in the table
                TObject cell = table.getLastCellContent(0);
                compared_to_true = compareCells(lhs_const, cell, op);
            } else if (op.is("<") || op.is("<=")) {
                // Find the minimum value in the table
                TObject cell = table.getFirstCellContent(0);
                compared_to_true = compareCells(lhs_const, cell, op);
            } else if (op.is("=")) {
                // Only true if rhs is a single value
                TObject cell = table.getSingleCellContent(0);
                compared_to_true = (cell != null && compareCells(lhs_const, cell, op));
            } else if (op.is("<>")) {
                // true only if lhs_cell is not found in column.
                compared_to_true = !table.columnContainsCell(0, lhs_const);
            } else {
                throw new Error("Don't understand operator '" + op + "' in ALL.");
            }

            // If matched return this table
            if (compared_to_true) {
                return this;
            }
            // No entries matches so return an empty table.
            return emptySelect();
        }

        Table source_table;
        int lhs_col_index;
        // Is the lhs expression a single variable?
        Variable lhs_var = lhs.getVariable();
        // NOTE: It'll be less common for this part to be called.
        if (lhs_var == null) {
            // This is a complex expression so make a FunctionTable as our new
            // source.
            DatabaseQueryContext db_context = (DatabaseQueryContext) context;
            FunctionTable fun_table = new FunctionTable(
                    this, new Expression[]{lhs}, new String[]{"1"}, db_context);
            source_table = fun_table;
            lhs_col_index = 0;
        } else {
            // The expression is an easy to resolve reference in this table.
            source_table = this;
            lhs_col_index = source_table.findFieldName(lhs_var);
            if (lhs_col_index == -1) {
                throw new Error("Can't find column '" + lhs_var + "'.");
            }
        }

        // Check that the first column of 'table' is of a compatible type with
        // source table column (lhs_col_index).
        // ISSUE: Should we convert to the correct type via a FunctionTable?
        DataTableColumnDef source_col = source_table.getColumnDefAt(lhs_col_index);
        DataTableColumnDef dest_col = table.getColumnDefAt(0);
        if (!source_col.getTType().comparableTypes(dest_col.getTType())) {
            throw new Error("The type of the sub-query expression " +
                    source_col.getSQLTypeString() + " is incompatible " +
                    "with the sub-query " + dest_col.getSQLTypeString() +
                    ".");
        }

        // We now have all the information to solve this query.
        // We work out as follows:
        //   For >, >= type ALL we find the highest value in 'table' and
        //   select from 'source' all the rows that are >, >= than the
        //   highest value.
        //   For <, <= type ALL we find the lowest value in 'table' and
        //   select from 'source' all the rows that are <, <= than the
        //   lowest value.
        //   For = type ALL we see if 'table' contains a single value.  If it
        //   does we select all from 'source' that equals the value, otherwise an
        //   empty table.
        //   For <> type ALL we use the 'not in' algorithm.

        IntegerVector select_vec;
        if (op.is(">") || op.is(">=")) {
            // Select the last from the set (the highest value),
            TObject highest_cell = table.getLastCellContent(0);
            // Select from the source table all rows that are > or >= to the
            // highest cell,
            select_vec = source_table.selectRows(lhs_col_index, op, highest_cell);
        } else if (op.is("<") || op.is("<=")) {
            // Select the first from the set (the lowest value),
            TObject lowest_cell = table.getFirstCellContent(0);
            // Select from the source table all rows that are < or <= to the
            // lowest cell,
            select_vec = source_table.selectRows(lhs_col_index, op, lowest_cell);
        } else if (op.is("=")) {
            // Select the single value from the set (if there is one).
            TObject single_cell = table.getSingleCellContent(0);
            if (single_cell != null) {
                // Select all from source_table all values that = this cell
                select_vec = source_table.selectRows(lhs_col_index, op, single_cell);
            } else {
                // No single value so return empty set (no value in LHS will equal
                // a value in RHS).
                return emptySelect();
            }
        } else if (op.is("<>")) {
            // Equiv. to NOT IN
            select_vec = INHelper.notIn(source_table, table, lhs_col_index, 0);
        } else {
            throw new Error("Don't understand operator '" + op + "' in ALL.");
        }

        // Make into a table to return.
        VirtualTable rtable = new VirtualTable(this);
        rtable.set(this, select_vec);

        // Query logging information
        if (Debug().isInterestedIn(Lvl.INFORMATION)) {
            Debug().write(Lvl.INFORMATION, this,
                    rtable + " = " + this + ".all(" +
                            lhs + ", " + op + ", " + table + ")");
        }

        return rtable;
    }


    // ---------- The original table functions ----------

    /**
     * Performs a natural join of this table with the given table.  This is
     * the same as calling the above 'join' with no conditional.
     */
    public final Table join(Table table) {

        boolean QUICK_NAT_JOIN = true;

        Table out_table;

        if (QUICK_NAT_JOIN) {

            // This implementation doesn't materialize the join
            out_table = new NaturallyJoinedTable(this, table);

        } else {

            Table[] tabs = new Table[2];
            tabs[0] = this;
            tabs[1] = table;
            IntegerVector[] row_sets = new IntegerVector[2];

            // Optimized trivial case, if either table has zero rows then result of
            // join will contain zero rows also.
            if (getRowCount() == 0 || table.getRowCount() == 0) {

                row_sets[0] = new IntegerVector(0);
                row_sets[1] = new IntegerVector(0);

            } else {

                // The natural join algorithm.
                IntegerVector this_row_set = new IntegerVector();
                IntegerVector table_row_set = new IntegerVector();

                // Get the set of all rows in the given table.
                IntegerVector table_selected_set = new IntegerVector();
                RowEnumeration e = table.rowEnumeration();
                while (e.hasMoreRows()) {
                    int row_index = e.nextRowIndex();
                    table_selected_set.addInt(row_index);
                }
                int table_selected_set_size = table_selected_set.size();

                // Join with the set of rows in this table.
                e = rowEnumeration();
                while (e.hasMoreRows()) {
                    int row_index = e.nextRowIndex();
                    for (int i = 0; i < table_selected_set_size; ++i) {
                        this_row_set.addInt(row_index);
                    }
                    table_row_set.append(table_selected_set);
                }

                // The row sets we are joining from each table.
                row_sets[0] = this_row_set;
                row_sets[1] = table_row_set;
            }

            // Create the new VirtualTable with the joined tables.
            VirtualTable virt_table = new VirtualTable(tabs);
            virt_table.set(tabs, row_sets);

            out_table = virt_table;

        }

        if (DEBUG_QUERY) {
            if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                Debug().write(Lvl.INFORMATION, this,
                        out_table + " = " + this + ".naturalJoin(" + table + " )");
            }
        }

        return out_table;

    }

    /**
     * Finds all rows in this table that are 'outside' the result in the
     * given table.  This is used in OUTER JOIN's.  We perform a normal join,
     * then determine unmatched joins with this function.  We can then create
     * an OuterTable with this result to make the completed table.
     * <p>
     * 'rtable' must be a decendent of this table.
     */
    public final VirtualTable outside(Table rtable) {

        // Form the row list for right hand table,
        IntegerVector row_list = new IntegerVector(rtable.getRowCount());
        RowEnumeration e = rtable.rowEnumeration();
        while (e.hasMoreRows()) {
            row_list.addInt(e.nextRowIndex());
        }
        int col_index = rtable.findFieldName(getResolvedVariable(0));
        rtable.setToRowTableDomain(col_index, row_list, this);

        // This row set
        IntegerVector this_table_set = new IntegerVector(getRowCount());
        e = rowEnumeration();
        while (e.hasMoreRows()) {
            this_table_set.addInt(e.nextRowIndex());
        }

        // 'row_list' is now the rows in this table that are in 'rtable'.
        // Sort both 'this_table_set' and 'row_list'
        this_table_set.quickSort();
        row_list.quickSort();

        // Find all rows that are in 'this_table_set' and not in 'row_list'
        IntegerVector result_list = new IntegerVector(96);
        int size = this_table_set.size();
        int row_list_index = 0;
        int row_list_size = row_list.size();
        for (int i = 0; i < size; ++i) {
            int this_val = this_table_set.intAt(i);
            if (row_list_index < row_list_size) {
                int in_val = row_list.intAt(row_list_index);
                if (this_val < in_val) {
                    result_list.addInt(this_val);
                } else if (this_val == in_val) {
                    while (row_list_index < row_list_size &&
                            row_list.intAt(row_list_index) == in_val) {
                        ++row_list_index;
                    }
                } else {
                    throw new Error("'this_val' > 'in_val'");
                }
            } else {
                result_list.addInt(this_val);
            }
        }

        // Return the new VirtualTable
        VirtualTable table = new VirtualTable(this);
        table.set(this, result_list);

        return table;
    }

    /**
     * Returns a new Table that is the union of the this table and the given
     * table.  A union operation will remove any duplicate rows.
     */
    public final Table union(Table table) {

        // Optimizations - handle trivial case of row count in one of the tables
        //   being 0.
        // NOTE: This optimization assumes this table and the unioned table are
        //   of the same type.
        if ((getRowCount() == 0 && table.getRowCount() == 0) ||
                table.getRowCount() == 0) {

            if (DEBUG_QUERY) {
                if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                    Debug().write(Lvl.INFORMATION, this,
                            this + " = " + this + ".union(" + table + " )");
                }
            }
            return this;
        } else if (getRowCount() == 0) {
            if (DEBUG_QUERY) {
                if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                    Debug().write(Lvl.INFORMATION, this,
                            table + " = " + this + ".union(" + table + " )");
                }
            }
            return table;
        }

        // First we merge this table with the input table.

        RawTableInformation raw1 = resolveToRawTable(new RawTableInformation());
        RawTableInformation raw2 = table.resolveToRawTable(new RawTableInformation());

// DIAGNOSTIC
//    IntegerVector[] rows1 = raw1.getRows();
//    IntegerVector[] rows2 = raw2.getRows();
//    System.out.println(rows1.length);
//    System.out.println(rows1[0]);
//    System.out.println(rows2.length);
//    System.out.println(rows2[0]);
//    System.out.println(raw1.getTables()[0]);
//    System.out.println(raw2.getTables()[0]);

        // This will throw an exception if the table types do not match up.

        raw1.union(raw2);

// DIAGNOSTIC
//    System.out.println("---");
//    rows1 = raw1.getRows();
//    System.out.println(rows1.length);
//    System.out.println(rows1[0]);
//    System.out.println(raw1.getTables()[0]);
//    System.out.println("--end--");

        // Now 'raw1' contains a list of uniquely merged rows (ie. the union).
        // Now make it into a new table and return the information.

        Table[] table_list = raw1.getTables();
        VirtualTable table_out = new VirtualTable(table_list);
        table_out.set(table_list, raw1.getRows());

// DIAGNOSTIC
//    RowEnumeration renum = table_out.rowEnumeration();
//    while (renum.hasMoreRows()) {
//      int rindex = renum.nextRowIndex();
//      System.out.println(table_out.getCellContents(0, rindex));
//    }

        if (DEBUG_QUERY) {
            if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                Debug().write(Lvl.INFORMATION, this,
                        table_out + " = " + this + ".union(" + table + " )");
            }
        }

        return table_out;
    }

    /**
     * Returns a new table with any duplicate rows in this table removed.
     *
     * @deprecated - not a proper SQL distinct.
     */
    public final VirtualTable distinct() {
        RawTableInformation raw = resolveToRawTable(new RawTableInformation());
        raw.removeDuplicates();

        Table[] table_list = raw.getTables();
        VirtualTable table_out = new VirtualTable(table_list);
        table_out.set(table_list, raw.getRows());

        if (DEBUG_QUERY) {
            if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                Debug().write(Lvl.INFORMATION, this,
                        table_out + " = " + this + ".distinct()");
            }
        }

        return table_out;
    }

    /**
     * Returns a new table that has only distinct rows in it.  This is an
     * expensive operation.  We sort over all the columns, then iterate through
     * the result taking out any duplicate rows.
     * <p>
     * The int array contains the columns to make distinct over.
     * <p>
     * NOTE: This will change the order of this table in the result.
     */
    public final Table distinct(int[] col_map) {
        IntegerVector result_list = new IntegerVector();
        IntegerVector row_list = orderedRowList(col_map);

        int r_count = row_list.size();
        int previous_row = -1;
        for (int i = 0; i < r_count; ++i) {
            int row_index = row_list.intAt(i);

            if (previous_row != -1) {

                boolean equal = true;
                // Compare cell in column in this row with previous row.
                for (int n = 0; n < col_map.length && equal; ++n) {
                    TObject c1 = getCellContents(col_map[n], row_index);
                    TObject c2 = getCellContents(col_map[n], previous_row);
                    equal = equal && (c1.compareTo(c2) == 0);
                }

                if (!equal) {
                    result_list.addInt(row_index);
                }
            } else {
                result_list.addInt(row_index);
            }

            previous_row = row_index;
        }

        // Return the new table with distinct rows only.
        VirtualTable vt = new VirtualTable(this);
        vt.set(this, result_list);

        if (DEBUG_QUERY) {
            if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                Debug().write(Lvl.INFORMATION, this,
                        vt + " = " + this + ".distinct(" + Arrays.toString(col_map) + ")");
            }
        }

        return vt;

    }


    /**
     * Helper function.  Returns the index in the String array of the given
     * string value.
     */
    private int indexStringArray(String val, String[] array) {
        for (int n = 0; n < array.length; ++n) {
            if (array[n].equals(val)) {
                return n;
            }
        }
        return -1;
    }


    /**
     * Returns true if the given column number contains the value given.
     */
    public final boolean columnContainsValue(int column, TObject ob) {
        return columnMatchesValue(column, Operator.get("="), ob);
    }

    /**
     * Returns true if the given column contains a value that the given
     * operator returns true for with the given value.
     */
    public final boolean columnMatchesValue(int column, Operator op, TObject ob) {
        IntegerVector ivec = selectRows(column, op, ob);
        return (ivec.size() > 0);
    }

    /**
     * Returns true if the given column contains all values that the given
     * operator returns true for with the given value.
     */
    public final boolean allColumnMatchesValue(int column,
                                               Operator op, TObject ob) {
        IntegerVector ivec = selectRows(column, op, ob);
        return (ivec.size() == getRowCount());
    }

    /**
     * Returns a table that is ordered by the given column numbers.  This
     * can be used by various functions from grouping to distinction to
     * ordering.  Always sorted by ascending.
     */
    public final Table orderByColumns(int[] col_map) {
        // Sort by the column list.
        Table work = this;
        for (int i = col_map.length - 1; i >= 0; --i) {
            work = work.orderByColumn(col_map[i], true);
        }
        // A nice post condition to check on.
        if (getRowCount() != work.getRowCount()) {
            throw new Error("Internal Error, row count != sorted row count");
        }

        return work;
    }

    /**
     * Returns an IntegerVector that represents the list of rows in this
     * table in sorted order by the given column map.
     */
    public final IntegerVector orderedRowList(int[] col_map) {
        Table work = orderByColumns(col_map);
        // 'work' is now sorted by the columns,
        // Get the rows in this tables domain,
        int r_count = getRowCount();
        IntegerVector row_list = new IntegerVector(r_count);
        RowEnumeration e = work.rowEnumeration();
        while (e.hasMoreRows()) {
            row_list.addInt(e.nextRowIndex());
        }

        work.setToRowTableDomain(0, row_list, this);
        return row_list;
    }


    /**
     * Returns a Table which is identical to this table, except it is sorted by
     * the given column name.  This means that if you access the rows
     * sequentually you will be reading the sorted order of the column.
     */
    public final VirtualTable orderByColumn(int col_index, boolean ascending) {
        // Check the field can be sorted
        DataTableColumnDef col_def = getColumnDefAt(col_index);

        IntegerVector rows = selectAll(col_index);

        // Reverse the list if we are not ascending
        if (ascending == false) {
            rows.reverse();
        }

        // We now has an int[] array of rows from this table to make into a
        // new table.

        VirtualTable table = new VirtualTable(this);
        table.set(this, rows);

        if (DEBUG_QUERY) {
            if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                Debug().write(Lvl.INFORMATION, this,
                        table + " = " + this + ".orderByColumn(" +
                                col_index + ", " + ascending + ")");
            }
        }

        return table;
    }

    public final VirtualTable orderByColumn(Variable column, boolean ascending) {
        int col_index = findFieldName(column);
        if (col_index == -1) {
            throw new Error("Unknown column in 'orderByColumn' ( " + column + " )");
        }
        return orderByColumn(col_index, ascending);
    }

    public final VirtualTable orderByColumn(Variable column) {
        return orderByColumn(column, true);
    }


    /**
     * This returns an object that can only access the cells that are in this
     * table, and has no other access to the 'Table' class's functionality.  The
     * purpose of this object is to provide a clean way to access the state of a
     * table without being able to access any of the row sorting
     * (SelectableScheme) methods that would return incorrect information in the
     * situation where the table locks (via LockingMechanism) were removed.
     * NOTE: The methods in this class will only work if this table has its
     *   rows locked via the 'lockRoot(int)' method.
     */
    public final TableAccessState getTableAccessState() {
        return new TableAccessState(this);
    }

    /**
     * Returns a set that respresents the list of multi-column row numbers
     * selected from the table given the condition.
     * <p>
     * NOTE: This can be used to exploit multi-column indexes if they exist.
     */
    final IntegerVector selectRows(int[] cols, Operator op, TObject[] cells) {
        // PENDING: Look for an multi-column index to make this a lot faster,
        if (cols.length > 1) {
            throw new Error("Multi-column select not supported.");
        }
        return selectRows(cols[0], op, cells[0]);
    }

    /**
     * Returns a set that represents the list of row numbers selected from the
     * table given the condition.
     */
    final IntegerVector selectRows(int column, Operator op, TObject cell) {
        // If the cell is of an incompatible type, return no results,
        TType col_type = getTTypeForColumn(column);
        if (!cell.getTType().comparableTypes(col_type)) {
            // Types not comparable, so return 0
            return new IntegerVector(0);
        }

        // Get the selectable scheme for this column
        SelectableScheme ss = getSelectableSchemeFor(column, column, this);

        // If the operator is a standard operator, use the interned SelectableScheme
        // methods.
        if (op.is("=")) {
            return ss.selectEqual(cell);
        } else if (op.is("<>")) {
            return ss.selectNotEqual(cell);
        } else if (op.is(">")) {
            return ss.selectGreater(cell);
        } else if (op.is("<")) {
            return ss.selectLess(cell);
        } else if (op.is(">=")) {
            return ss.selectGreaterOrEqual(cell);
        } else if (op.is("<=")) {
            return ss.selectLessOrEqual(cell);
        }

        // If it's not a standard operator (such as IS, NOT IS, etc) we generate the
        // range set especially.
        SelectableRangeSet range_set = new SelectableRangeSet();
        range_set.intersect(op, cell);
        return ss.selectRange(range_set.toSelectableRangeArray());
    }

    /**
     * Selects the rows in a table column between two minimum and maximum bounds.
     * This is all rows which are >= min_cell and < max_cell.
     * <p>
     * NOTE: The returns IntegerVector _must_ be sorted be the 'column' cells.
     */
    IntegerVector selectRows(int column, TObject min_cell, TObject max_cell) {
        // Check all the tables are comparable
        TType col_type = getTTypeForColumn(column);
        if (!min_cell.getTType().comparableTypes(col_type) ||
                !max_cell.getTType().comparableTypes(col_type)) {
            // Types not comparable, so return 0
            return new IntegerVector(0);
        }

        SelectableScheme ss = getSelectableSchemeFor(column, column, this);
        return ss.selectBetween(min_cell, max_cell);
    }

    /**
     * Selects all the rows where the given column matches the regular
     * expression.  This uses the static class 'PatternSearch' to perform the
     * operation.
     * <p>
     * This method must guarentee the result is ordered by the given column.
     */
    final IntegerVector selectFromRegex(int column, Operator op, TObject ob) {
        if (ob.isNull()) {
            return new IntegerVector(0);
        }

        return PatternSearch.regexSearch(this, column, ob.getObject().toString());
    }

    /**
     * Selects all the rows where the given column matches the given pattern.
     * This uses the static class 'PatternSearch' to perform these operations.
     * 'operation' will be either Condition.LIKE or Condition.NOT_LIKE.
     * NOTE: The returns IntegerVector _must_ be sorted be the 'column' cells.
     */
    final IntegerVector selectFromPattern(int column, Operator op, TObject ob) {
        if (ob.isNull()) {
            return new IntegerVector();
        }

        if (op.is("not like")) {
            // How this works:
            //   Find the set or rows that are like the pattern.
            //   Find the complete set of rows in the column.
            //   Sort the 'like' rows
            //   For each row that is in the original set and not in the like set,
            //     add to the result list.
            //   Result is the set of not like rows ordered by the column.
            IntegerVector like_set =
                    PatternSearch.search(this, column, ob.toString());
            // Don't include NULL values
            TObject null_cell = new TObject(ob.getTType(), null);
            IntegerVector original_set =
                    selectRows(column, Operator.get("is not"), null_cell);
            int vec_size = Math.max(4, (original_set.size() - like_set.size()) + 4);
            IntegerVector result_set = new IntegerVector(vec_size);
            like_set.quickSort();
            int size = original_set.size();
            for (int i = 0; i < size; ++i) {
                int val = original_set.intAt(i);
                // If val not in like set, add to result
                if (like_set.sortedIntCount(val) == 0) {
                    result_set.addInt(val);
                }
            }
            return result_set;
        } else { // if (op.is("like")) {
            return PatternSearch.search(this, column, ob.toString());
        }
    }

    /**
     * Given a table and column (from this table), this returns all the rows
     * from this table that are also in the first column of the given table.
     * This is the basis of a fast 'in' process.
     */
    final IntegerVector allRowsIn(int column, Table table) {
        IntegerVector iv = INHelper.in(this, table, column, 0);
        return iv;
    }

    /**
     * Given a table and column (from this table), this returns all the rows
     * from this table that are not in the first column of the given table.
     * This is the basis of a fast 'not in' process.
     */
    final IntegerVector allRowsNotIn(int column, Table table) {
        return INHelper.notIn(this, table, column, 0);
    }

    /**
     * Returns an array that represents the sorted order of this table by
     * the given column number.
     */
    public final IntegerVector selectAll(int column) {
        SelectableScheme ss = getSelectableSchemeFor(column, column, this);
        return ss.selectAll();
    }

    /**
     * Returns a list of rows that represents the enumerator order of this
     * table.
     */
    public final IntegerVector selectAll() {
        IntegerVector list = new IntegerVector(getRowCount());
        RowEnumeration en = rowEnumeration();
        while (en.hasMoreRows()) {
            list.addInt(en.nextRowIndex());
        }
        return list;
    }

    /**
     * Returns an array that represents the sorted order of this table of all
     * values in the given SelectableRange objects of the given column index.
     * If there is an index on the column, the result can be found very quickly.
     * The range array must be normalized (no overlapping ranges).
     */
    public final IntegerVector selectRange(int column,
                                           SelectableRange[] ranges) {
        SelectableScheme ss = getSelectableSchemeFor(column, column, this);
        return ss.selectRange(ranges);
    }

    /**
     * Returns an array that represents the last sorted element(s) of the given
     * column number.
     */
    public final IntegerVector selectLast(int column) {
        SelectableScheme ss = getSelectableSchemeFor(column, column, this);
        return ss.selectLast();
    }

    /**
     * Returns an array that represents the first sorted element(s) of the given
     * column number.
     */
    public final IntegerVector selectFirst(int column) {
        SelectableScheme ss = getSelectableSchemeFor(column, column, this);
        return ss.selectFirst();
    }

    /**
     * Returns an array that represents the rest of the sorted element(s) of the
     * given column number.  (not the 'first' set).
     */
    public final IntegerVector selectRest(int column) {
        SelectableScheme ss = getSelectableSchemeFor(column, column, this);
        return ss.selectNotFirst();
    }

    /**
     * Convenience, returns a TObject[] array given a single TObject, or
     * null if the TObject is null (not if TObject represents a null value).
     */
    private TObject[] singleArrayCellMap(TObject cell) {
        return cell == null ? null : new TObject[]{cell};
    }

    /**
     * Returns the TObject value that represents the first item in the set or
     * null if there are no items in the column set.
     */
    public final TObject getFirstCellContent(int column) {
        IntegerVector ivec = selectFirst(column);
        if (ivec.size() > 0) {
            return getCellContents(column, ivec.intAt(0));
        }
        return null;
    }

    /**
     * Returns the TObject value that represents the first item in the set or
     * null if there are no items in the column set.
     */
    public final TObject[] getFirstCellContent(int[] col_map) {
        if (col_map.length > 1) {
            throw new Error("Multi-column getLastCellContent not supported.");
        }
        return singleArrayCellMap(getFirstCellContent(col_map[0]));
    }

    /**
     * Returns the TObject value that represents the last item in the set or
     * null if there are no items in the column set.
     */
    public final TObject getLastCellContent(int column) {
        IntegerVector ivec = selectLast(column);
        if (ivec.size() > 0) {
            return getCellContents(column, ivec.intAt(0));
        }
        return null;
    }

    /**
     * Returns the TObject value that represents the last item in the set or
     * null if there are no items in the column set.
     */
    public final TObject[] getLastCellContent(int[] col_map) {
        if (col_map.length > 1) {
            throw new Error("Multi-column getLastCellContent not supported.");
        }
        return singleArrayCellMap(getLastCellContent(col_map[0]));
    }

    /**
     * If the given column contains all items of the same value, this method
     * returns the value.  If it doesn't, or the column set is empty it returns
     * null.
     */
    public final TObject getSingleCellContent(int column) {
        IntegerVector ivec = selectFirst(column);
        int sz = ivec.size();
        if (sz == getRowCount() && sz > 0) {
            return getCellContents(column, ivec.intAt(0));
        }
        return null;
    }

    /**
     * If the given column contains all items of the same value, this method
     * returns the value.  If it doesn't, or the column set is empty it returns
     * null.
     */
    public final TObject[] getSingleCellContent(int[] col_map) {
        if (col_map.length > 1) {
            throw new Error("Multi-column getSingleCellContent not supported.");
        }
        return singleArrayCellMap(getSingleCellContent(col_map[0]));
    }

    /**
     * Returns true if the given cell is found in the table.
     */
    public final boolean columnContainsCell(int column, TObject cell) {
        IntegerVector ivec = selectRows(column, Operator.get("="), cell);
        return ivec.size() > 0;
    }

    /**
     * Compares cell1 with cell2 and if the given operator evalutes to true then
     * returns true, otherwise false.
     */
    public static boolean compareCells(
            TObject ob1, TObject ob2, Operator op) {
        TObject result = op.eval(ob1, ob2, null, null, null);
        // NOTE: This will be a NullPointerException if the result is not a
        //   boolean type.
        return result.toBoolean();
    }

    /**
     * Assuming this table is a 2 column key/value table, and the first column
     * is a string, this will convert it into a map.  The returned map can
     * then be used to access values in the second column.
     */
    public Map toMap() {
        if (getColumnCount() == 2) {
            HashMap map = new HashMap();
            RowEnumeration en = rowEnumeration();
            while (en.hasMoreRows()) {
                int row_index = en.nextRowIndex();
                TObject key = getCellContents(0, row_index);
                TObject value = getCellContents(1, row_index);
                map.put(key.getObject().toString(), value.getObject());
            }
            return map;
        } else {
            throw new Error("Table must have two columns.");
        }
    }


    // Stores col name -> col index lookups
    private HashMap col_name_lookup;
    private final Object COL_LOOKUP_LOCK = new Object();

    /**
     * A faster way to find a column index given a string column name.  This
     * caches column name -> column index in a HashMap.
     */
    public final int fastFindFieldName(Variable col) {
        synchronized (COL_LOOKUP_LOCK) {
            if (col_name_lookup == null) {
                col_name_lookup = new HashMap(30);
            }
            Object ob = col_name_lookup.get(col);
            if (ob == null) {
                int ci = findFieldName(col);
                col_name_lookup.put(col, ci);
                return ci;
            } else {
                return (Integer) ob;
            }
        }
    }

    /**
     * Returns a TableVariableResolver object for this table.
     */
    final TableVariableResolver getVariableResolver() {
        return new TableVariableResolver();
    }


    // ---------- Inner classes ----------

    /**
     * An implementation of VariableResolver that we can use to resolve column
     * names in this table to cells for a specific row.
     */
    final class TableVariableResolver implements VariableResolver {

        private int row_index = -1;

        public void setRow(int row_index) {
            this.row_index = row_index;
        }

        private int findColumnName(Variable variable) {
            int col_index = fastFindFieldName(variable);
            if (col_index == -1) {
                throw new Error("Can't find column: " + variable);
            }
            return col_index;
        }

        // --- Implemented ---

        public int setID() {
            return row_index;
        }

        public TObject resolve(Variable variable) {
            return getCellContents(findColumnName(variable), row_index);
        }

        public TType returnTType(Variable variable) {
            return getTTypeForColumn(variable);
        }

    }

    /**
     * Returns a string that represents this table.
     */
    public String toString() {
        String name = "VT" + hashCode();
        if (this instanceof AbstractDataTable) {
            name = ((AbstractDataTable) this).getTableName().toString();
        }
        return name + "[" + getRowCount() + "]";
    }

    /**
     * Prints a graph of the table hierarchy to the stream.
     */
    public void printGraph(PrintStream out, int indent) {
        for (int i = 0; i < indent; ++i) {
            out.print(' ');
        }
        out.println("T[" + getClass() + "]");
    }

}
