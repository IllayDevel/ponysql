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

import com.pony.database.global.Types;
import com.pony.database.global.ByteLongObject;
import com.pony.util.Cache;
import com.pony.util.IntegerVector;
import com.pony.debug.*;
import com.pony.util.BigNumber;

import java.util.Date;

/**
 * A table that has a number of columns and as many rows as the refering
 * table.  Tables of this type are used to construct aggregate and function
 * columns based on an expression.  They are joined with the result table in
 * the last part of the query processing.
 * <p>
 * For example, a query like 'select id, id * 2, 8 * 9 from Part' the
 * columns 'id * 2' and '8 * 9' would be formed from this table.
 * <p>
 * SYNCHRONIZATION ISSUE: Instances of this object are NOT thread safe.  The
 *   reason it's not is because if 'getCellContents' is used concurrently it's
 *   possible for the same value to be added into the cache causing an error.
 *   It is not expected that this object will be shared between threads.
 *
 * @author Tobias Downer
 */

public class FunctionTable extends DefaultDataTable {

    /**
     * The key used to make distinct unique ids for FunctionTables.
     * <p>
     * NOTE: This is a thread-safe static mutable variable.
     */
    private static int UNIQUE_KEY_SEQ = 0;

    /**
     * The table name given to all function tables.
     */
    private static final TableName FUNCTION_TABLE_NAME =
            new TableName(null, "FUNCTIONTABLE");

    /**
     * A unique id given to this FunctionTable when it is created.  No two
     * FunctionTable objects may have the same number.  This number is between
     * 0 and 260 million.
     */
    private int unique_id;

    /**
     * The DataTableDef object that describes the columns in this function
     * table.
     */
    private DataTableDef fun_table_def;

    /**
     * The table that this function table cross references.  This is not a
     * parent table, but more like the table we will eventually be joined with.
     */
    private Table cross_ref_table;

    /**
     * The TableVariableResolver for the table we are cross referencing.
     */
    private TableVariableResolver cr_resolver;

    /**
     * The TableGroupResolver for the table.
     */
    private TableGroupResolver group_resolver;

    /**
     * The list of expressions that are evaluated to form each column.
     */
    private Expression[] exp_list;

    /**
     * Some information about the expression list.  If the value is 0 then the
     * column is simple to solve and shouldn't be cached.
     */
    private byte[] exp_info;

    /**
     * The lookup mapping for row->group_index used for grouping.
     */
    private IntegerVector group_lookup;

    /**
     * The group row links.  Iterate through this to find all the rows in a
     * group until bit 31 set.
     */
    private IntegerVector group_links;

    /**
     * Whether the whole table is a group.
     */
    private boolean whole_table_as_group = false;

    /**
     * If the whole table is a group, this is the grouping rows.  This is
     * obtained via 'selectAll' of the reference table.
     */
    private IntegerVector whole_table_group;
    /**
     * The total size of the whole table group size.
     */
    private int whole_table_group_size;
    /**
     * If the whole table is a simple enumeration (row index is 0 to getRowCount)
     * then this is true.
     */
    private boolean whole_table_is_simple_enum;

    /**
     * The context of this function table.
     */
    private QueryContext context;


    /**
     * Constructs the FunctionTable.
     */
    public FunctionTable(Table cross_ref_table, Expression[] in_exp_list,
                         String[] col_names, DatabaseQueryContext context) {
        super(context.getDatabase());

        // Make sure we are synchronized over the class.
        synchronized (FunctionTable.class) {
            unique_id = UNIQUE_KEY_SEQ;
            ++UNIQUE_KEY_SEQ;
        }
        unique_id = (unique_id & 0x0FFFFFFF) | 0x010000000;

        this.context = context;

        this.cross_ref_table = cross_ref_table;
        cr_resolver = cross_ref_table.getVariableResolver();
        cr_resolver.setRow(0);

        // Create a DataTableDef object for this function table.
        fun_table_def = new DataTableDef();
        fun_table_def.setTableName(FUNCTION_TABLE_NAME);

        exp_list = new Expression[in_exp_list.length];
        exp_info = new byte[in_exp_list.length];

        // Create a new DataTableColumnDef for each expression, and work out if the
        // expression is simple or not.
        for (int i = 0; i < in_exp_list.length; ++i) {
            Expression expr = in_exp_list[i];
            // Examine the expression and determine if it is simple or not
            if (expr.isConstant() && !expr.hasAggregateFunction(context)) {
                // If expression is a constant, solve it
                TObject result = expr.evaluate(null, null, context);
                expr = new Expression(result);
                exp_list[i] = expr;
                exp_info[i] = 1;
            } else {
                // Otherwise must be dynamic
                exp_list[i] = expr;
                exp_info[i] = 0;
            }
            // Make the column def
            DataTableColumnDef column = new DataTableColumnDef();
            column.setName(col_names[i]);
            column.setFromTType(expr.returnTType(cr_resolver, context));
            fun_table_def.addVirtualColumn(column);
        }

        // Make sure the table def isn't changed from this point on.
        fun_table_def.setImmutable();

        // Function tables are the size of the referring table.
        row_count = cross_ref_table.getRowCount();

        // Set schemes to 'blind search'.
        blankSelectableSchemes(1);
    }

    public FunctionTable(Expression[] exp_list, String[] col_names,
                         DatabaseQueryContext context) {
        this(context.getDatabase().getSingleRowTable(),
                exp_list, col_names, context);
    }

    /**
     * Return a TObject that represents the value of the 'column', 'row' of
     * this table.  If 'cache' is not null then the resultant value is added to
     * the cache.  If 'cache' is null, no caching happens.
     */
    private TObject calcValue(int column, int row, DataCellCache cache) {
        cr_resolver.setRow(row);
        if (group_resolver != null) {
            group_resolver.setUpGroupForRow(row);
        }
        Expression expr = exp_list[column];
        TObject cell = expr.evaluate(group_resolver, cr_resolver, context);
        if (cache != null) {
            cache.put(unique_id, row, column, cell);
        }
        return cell;
    }

    // ------ Public methods ------

    /**
     * Sets the whole reference table as a single group.
     */
    public void setWholeTableAsGroup() {
        whole_table_as_group = true;

        whole_table_group_size = getReferenceTable().getRowCount();

        // Set up 'whole_table_group' to the list of all rows in the reference
        // table.
        RowEnumeration en = getReferenceTable().rowEnumeration();
        whole_table_is_simple_enum = en instanceof SimpleRowEnumeration;
        if (!whole_table_is_simple_enum) {
            whole_table_group = new IntegerVector(getReferenceTable().getRowCount());
            while (en.hasMoreRows()) {
                whole_table_group.addInt(en.nextRowIndex());
            }
        }

        // Set up a group resolver for this method.
        group_resolver = new TableGroupResolver();
    }

    /**
     * Creates a grouping matrix for the given tables.  The grouping matrix
     * is arranged so that each row of the referring table that is in the
     * group is given a number that refers to the top group entry in the
     * group list.  The group list is a linked integer list that chains through
     * each row item in the list.
     */
    public void createGroupMatrix(Variable[] col_list) {
        // If we have zero rows, then don't bother creating the matrix.
        if (getRowCount() <= 0 || col_list.length <= 0) {
            return;
        }

        Table root_table = getReferenceTable();
        int r_count = root_table.getRowCount();
        int[] col_lookup = new int[col_list.length];
        for (int i = col_list.length - 1; i >= 0; --i) {
            col_lookup[i] = root_table.findFieldName(col_list[i]);
        }
        IntegerVector row_list = root_table.orderedRowList(col_lookup);

        // 'row_list' now contains rows in this table sorted by the columns to
        // group by.

        // This algorithm will generate two lists.  The group_lookup list maps
        // from rows in this table to the group number the row belongs in.  The
        // group number can be used as an index to the 'group_links' list that
        // contains consequtive links to each row in the group until -1 is reached
        // indicating the end of the group;

        group_lookup = new IntegerVector(r_count);
        group_links = new IntegerVector(r_count);
        int current_group = 0;
        int previous_row = -1;
        for (int i = 0; i < r_count; ++i) {
            int row_index = row_list.intAt(i);

            if (previous_row != -1) {

                boolean equal = true;
                // Compare cell in column in this row with previous row.
                for (int n = 0; n < col_lookup.length && equal; ++n) {
                    TObject c1 = root_table.getCellContents(col_lookup[n], row_index);
                    TObject c2 =
                            root_table.getCellContents(col_lookup[n], previous_row);
                    equal = equal && (c1.compareTo(c2) == 0);
                }

                if (!equal) {
                    // If end of group, set bit 15
                    group_links.addInt(previous_row | 0x040000000);
                    current_group = group_links.size();
                } else {
                    group_links.addInt(previous_row);
                }

            }

            group_lookup.placeIntAt(current_group, row_index);   // (val, pos)

            previous_row = row_index;
        }
        // Add the final row.
        group_links.addInt(previous_row | 0x040000000);

        // Set up a group resolver for this method.
        group_resolver = new TableGroupResolver();

    }


    // ------ Methods intended for use by grouping functions ------

    /**
     * Returns the Table this function is based on.  We need to provide this
     * method for aggregate functions.
     */
    public Table getReferenceTable() {
        return cross_ref_table;
    }

    /**
     * Returns the group of the row at the given index.
     */
    public int rowGroup(int row_index) {
        return group_lookup.intAt(row_index);
    }

    /**
     * The size of the group with the given number.
     */
    public int groupSize(int group_number) {
        int group_size = 1;
        int i = group_links.intAt(group_number);
        while ((i & 0x040000000) == 0) {
            ++group_size;
            ++group_number;
            i = group_links.intAt(group_number);
        }
        return group_size;
    }

    /**
     * Returns an IntegerVector that represents the list of all rows in the
     * group the index is at.
     */
    public IntegerVector groupRows(int group_number) {
        IntegerVector ivec = new IntegerVector();
        int i = group_links.intAt(group_number);
        while ((i & 0x040000000) == 0) {
            ivec.addInt(i);
            ++group_number;
            i = group_links.intAt(group_number);
        }
        ivec.addInt(i & 0x03FFFFFFF);
        return ivec;
    }

    /**
     * Returns a Table that is this function table merged with the cross
     * reference table.  The result table includes only one row from each
     * group.
     * <p>
     * The 'max_column' argument is optional (can be null).  If it's set to a
     * column in the reference table, then the row with the max value from the
     * group is used as the group row.  For example, 'Part.id' will return the
     * row with the maximum part.id from each group.
     */
    public Table mergeWithReference(Variable max_column) {
        Table table = getReferenceTable();

        IntegerVector row_list;

        if (whole_table_as_group) {
            // Whole table is group, so take top entry of table.

            row_list = new IntegerVector(1);
            RowEnumeration row_enum = table.rowEnumeration();
            if (row_enum.hasMoreRows()) {
                row_list.addInt(row_enum.nextRowIndex());
            } else {
                // MAJOR HACK: If the referencing table has no elements then we choose
                //   an arbitary index from the reference table to merge so we have
                //   at least one element in the table.
                //   This is to fix the 'SELECT COUNT(*) FROM empty_table' bug.
                row_list.addInt(Integer.MAX_VALUE - 1);
            }
        } else if (table.getRowCount() == 0) {
            row_list = new IntegerVector(0);
        } else if (group_links != null) {
            // If we are grouping, reduce down to only include one row from each
            // group.
            if (max_column == null) {
                row_list = topFromEachGroup();
            } else {
                int col_num = getReferenceTable().findFieldName(max_column);
                row_list = maxFromEachGroup(col_num);
            }
        } else {
            // OPTIMIZATION: This should be optimized.  It should be fairly trivial
            //   to generate a Table implementation that efficiently merges this
            //   function table with the reference table.

            // This means there is no grouping, so merge with entire table,
            int r_count = table.getRowCount();
            row_list = new IntegerVector(r_count);
            RowEnumeration en = table.rowEnumeration();
            while (en.hasMoreRows()) {
                row_list.addInt(en.nextRowIndex());
            }
        }

        // Create a virtual table that's the new group table merged with the
        // functions in this...

        Table[] tabs = new Table[]{table, this};
        IntegerVector[] row_sets = new IntegerVector[]{row_list, row_list};

        VirtualTable out_table = new VirtualTable(tabs);
        out_table.set(tabs, row_sets);

        // Output this as debugging information
        if (DEBUG_QUERY) {
            if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                Debug().write(Lvl.INFORMATION, this,
                        out_table + " = " + this + ".mergeWithReference(" +
                                getReferenceTable() + ", " + max_column + " )");
            }
        }

        table = out_table;
        return table;
    }

    // ------ Package protected methods -----

    /**
     * Returns a list of rows that represent one row from each distinct group
     * in this table.  This should be used to construct a virtual table of
     * rows from each distinct group.
     */
    IntegerVector topFromEachGroup() {
        IntegerVector extract_rows = new IntegerVector();
        int size = group_links.size();
        boolean take = true;
        for (int i = 0; i < size; ++i) {
            int r = group_links.intAt(i);
            if (take) {
                extract_rows.addInt(r & 0x03FFFFFFF);
            }
            take = (r & 0x040000000) != 0;
        }

        return extract_rows;
    }


    /**
     * Returns a list of rows that represent the maximum row of the given column
     * from each distinct group in this table.  This should be used to construct
     * a virtual table of rows from each distinct group.
     */
    IntegerVector maxFromEachGroup(int col_num) {

        final Table ref_tab = getReferenceTable();

        IntegerVector extract_rows = new IntegerVector();
        int size = group_links.size();

        int to_take_in_group = -1;
        TObject max = null;

        boolean take = true;
        for (int i = 0; i < size; ++i) {
            int r = group_links.intAt(i);

            int act_r_index = r & 0x03FFFFFFF;
            TObject cell = ref_tab.getCellContents(col_num, act_r_index);
            if (max == null || cell.compareTo(max) > 0) {
                max = cell;
                to_take_in_group = act_r_index;
            }
            if ((r & 0x040000000) != 0) {
                extract_rows.addInt(to_take_in_group);
                max = null;
            }

        }

        return extract_rows;
    }

    // ------ Methods that are implemented for Table interface ------

    /**
     * Returns the DataTableDef object that represents the columns in this
     * function table.
     */
    public DataTableDef getDataTableDef() {
        return fun_table_def;
    }

    /**
     * Returns an object that represents the information in the given cell
     * in the table.  This can be used to obtain information about the given
     * table cells.
     */
    public TObject getCellContents(int column, int row) {

        // [ FUNCTION TABLE CACHING NOW USES THE GLOBAL CELL CACHING MECHANISM ]
        // Check if in the cache,
        DataCellCache cache = getDatabase().getDataCellCache();
        // Is the column worth caching, and is caching enabled?
        if (exp_info[column] == 0 && cache != null) {
            TObject cell = cache.get(unique_id, row, column);
            if (cell != null) {
                // In the cache so return the cell.
                return cell;
            } else {
                // Not in the cache so calculate the value and put it in the cache.
                cell = calcValue(column, row, cache);
                return cell;
            }
        } else {
            // Caching is not enabled
            return calcValue(column, row, null);
        }

    }

    /**
     * Returns an Enumeration of the rows in this table.
     * Each call to 'nextRowIndex' returns the next valid row index in the table.
     */
    public RowEnumeration rowEnumeration() {
        return new SimpleRowEnumeration(row_count);
    }

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
    void addDataTableListener(DataTableListener listener) {
        // Add a data table listener to the reference table.
        // NOTE: This will cause the reference table to have the same listener
        //   registered twice if the 'mergeWithReference' method is used.  While
        //   this isn't perfect behaviour, it means if 'mergeWithReference' isn't
        //   used, we still will be notified of changes in the reference table
        //   which will alter the values in this table.
        getReferenceTable().addDataTableListener(listener);
    }

    /**
     * Removes a DataTableListener from the DataTable objects at the root of
     * this table tree hierarchy.  If this table represents the join of a
     * number of tables, then the DataTableListener is removed from all the
     * DataTable objects at the root.
     */
    void removeDataTableListener(DataTableListener listener) {
        // Removes a data table listener to the reference table.
        // ( see notes above... )
        getReferenceTable().removeDataTableListener(listener);
    }

    /**
     * Locks the root table(s) of this table so that it is impossible to
     * overwrite the underlying rows that may appear in this table.
     * This is used when cells in the table need to be accessed 'outside' the
     * lock.  So we may have late access to cells in the table.
     * 'lock_key' is a given key that will also unlock the root table(s).
     * NOTE: This is nothing to do with the 'LockingMechanism' object.
     */
    public void lockRoot(int lock_key) {
        // We lock the reference table.
        // NOTE: This cause the reference table to lock twice when we use the
        //  'mergeWithReference' method.  While this isn't perfect behaviour, it
        //  means if 'mergeWithReference' isn't used, we still maintain a safe
        //  level of locking.
        getReferenceTable().lockRoot(lock_key);
    }

    /**
     * Unlocks the root tables so that the underlying rows may
     * once again be used if they are not locked and have been removed.  This
     * should be called some time after the rows have been locked.
     */
    public void unlockRoot(int lock_key) {
        // We unlock the reference table.
        // NOTE: This cause the reference table to unlock twice when we use the
        //  'mergeWithReference' method.  While this isn't perfect behaviour, it
        //  means if 'mergeWithReference' isn't used, we still maintain a safe
        //  level of locking.
        getReferenceTable().unlockRoot(lock_key);
    }

    /**
     * Returns true if the table has its row roots locked (via the lockRoot(int)
     * method.
     */
    public boolean hasRootsLocked() {
        return getReferenceTable().hasRootsLocked();
    }

    // ---------- Convenience statics ----------

    /**
     * Returns a FunctionTable that has a single Expression evaluated in it.
     * The column name is 'result'.
     */
    public static Table resultTable(DatabaseQueryContext context,
                                    Expression expression) {
        Expression[] exp = new Expression[]{expression};
        String[] names = new String[]{"result"};
        Table function_table = new FunctionTable(exp, names, context);
        SubsetColumnTable result = new SubsetColumnTable(function_table);

        int[] map = new int[]{0};
        Variable[] vars = new Variable[]{new Variable("result")};
        result.setColumnMap(map, vars);

        return result;
    }

    /**
     * Returns a FunctionTable that has a single TObject in it.
     * The column title is 'result'.
     */
    public static Table resultTable(DatabaseQueryContext context, TObject ob) {
        Expression result_exp = new Expression();
        result_exp.addElement(ob);
        return resultTable(context, result_exp);
    }

    /**
     * Returns a FunctionTable that has a single Object in it.
     * The column title is 'result'.
     */
    public static Table resultTable(DatabaseQueryContext context, Object ob) {
        return resultTable(context, TObject.objectVal(ob));
    }

    /**
     * Returns a FunctionTable that has an int value made into a BigDecimal.
     * The column title is 'result'.
     */
    public static Table resultTable(DatabaseQueryContext context,
                                    int result_val) {
        return resultTable(context, BigNumber.fromInt(result_val));
    }


    // ---------- Inner classes ----------

    /**
     * Group resolver.  This is used to resolve group information in the
     * refering table.
     */
    final class TableGroupResolver implements GroupResolver {

        /**
         * The IntegerVector that represents the group we are currently
         * processing.
         */
        private IntegerVector group;

//    /**
//     * The group row index we are current set at.
//     */
//    private int group_row_index;

        /**
         * The current group number.
         */
        private int group_number = -1;

        /**
         * A VariableResolver that can resolve variables within a set of a group.
         */
        private TableGVResolver tgv_resolver;


        /**
         * Creates a resolver that resolves variables within a set of the group.
         */
        private TableGVResolver createVariableResolver() {
            if (tgv_resolver != null) {
                return tgv_resolver;
            }
            tgv_resolver = new TableGVResolver();
            return tgv_resolver;
        }


        /**
         * Ensures that 'group' is set up.
         */
        private void ensureGroup() {
            if (group == null) {
                if (group_number == -2) {
                    group = whole_table_group;
//          // ISSUE: Unsafe calls if reference table is a DataTable.
//          group = new IntegerVector(getReferenceTable().getRowCount());
//          RowEnumeration renum = getReferenceTable().rowEnumeration();
//          while (renum.hasMoreRows()) {
//            group.addInt(renum.nextRowIndex());
//          }
                } else {
                    group = groupRows(group_number);
                }
            }
        }

        /**
         * Given a row index, this will setup the information in this resolver
         * to solve for this group.
         */
        public void setUpGroupForRow(int row_index) {
            if (whole_table_as_group) {
                if (group_number != -2) {
                    group_number = -2;
                    group = null;
                }
            } else {
                int g = rowGroup(row_index);
                if (g != group_number) {
                    group_number = g;
                    group = null;
                }
            }
        }

        public int groupID() {
            return group_number;
        }

        public int size() {
            if (group_number == -2) {
                return whole_table_group_size;
//        return whole_table_group.size();
//        // ISSUE: Unsafe call if reference table is a DataTable.
//        return getReferenceTable().getRowCount();
            } else if (group != null) {
                return group.size();
            } else {
                return groupSize(group_number);
            }
        }

        public TObject resolve(Variable variable, int set_index) {
//      String col_name = variable.getName();

            int col_index = getReferenceTable().fastFindFieldName(variable);
            if (col_index == -1) {
                throw new Error("Can't find column: " + variable);
            }

            ensureGroup();

            int row_index = set_index;
            if (group != null) {
                row_index = group.intAt(set_index);
            }
            TObject cell = getReferenceTable().getCellContents(col_index, row_index);

            return cell;
        }

        public VariableResolver getVariableResolver(int set_index) {
            TableGVResolver resolver = createVariableResolver();
            resolver.setIndex(set_index);
            return resolver;
        }

        // ---------- Inner classes ----------

        private class TableGVResolver implements VariableResolver {

            private int set_index;

            void setIndex(int set_index) {
                this.set_index = set_index;
            }

            // ---------- Implemented from VariableResolver ----------

            public int setID() {
                throw new Error("setID not implemented here...");
            }

            public TObject resolve(Variable variable) {
                return TableGroupResolver.this.resolve(variable, set_index);
            }

            public TType returnTType(Variable variable) {
                int col_index = getReferenceTable().fastFindFieldName(variable);
                if (col_index == -1) {
                    throw new Error("Can't find column: " + variable);
                }

                return getReferenceTable().getDataTableDef().columnAt(
                        col_index).getTType();
            }

        }

    }

}
