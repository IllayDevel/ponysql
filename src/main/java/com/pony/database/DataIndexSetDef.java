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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;

/**
 * Represents the meta-data for a set of indexes of a table.
 *
 * @author Tobias Downer
 */

public class DataIndexSetDef {

    /**
     * The TableName this index set meta data is for.
     */
    private final TableName table_name;

    /**
     * The list of indexes in the table.
     */
    private final ArrayList index_list;

    /**
     * True if this object is immutable.
     */
    private boolean immutable;

    /**
     * Constructor.
     */
    public DataIndexSetDef(TableName table_name) {
        this.table_name = table_name;
        index_list = new ArrayList();
        immutable = false;
    }

    public DataIndexSetDef(DataIndexSetDef def) {
        this.table_name = def.table_name;
        index_list = new ArrayList();
        for (int i = 0; i < def.indexCount(); ++i) {
            index_list.add(new DataIndexDef(def.indexAt(i)));
        }
        immutable = false;
    }

    /**
     * Sets the immutable flag.
     */
    public void setImmutable() {
        this.immutable = true;
    }

    /**
     * Adds a DataIndexDef to this table.
     */
    public void addDataIndexDef(DataIndexDef def) {
        if (!immutable) {
            index_list.add(def);
        } else {
            throw new RuntimeException("Tried to add index to immutable def.");
        }
    }

    /**
     * Removes a DataIndexDef to this table.
     */
    public void removeDataIndexDef(int i) {
        if (!immutable) {
            index_list.remove(i);
        } else {
            throw new RuntimeException("Tried to add index to immutable def.");
        }
    }

    /**
     * Returns the total number of index in this table.
     */
    public int indexCount() {
        return index_list.size();
    }

    /**
     * Returns the DataIndexDef at the given index in this list.
     */
    public DataIndexDef indexAt(int i) {
        return (DataIndexDef) index_list.get(i);
    }

    /**
     * Finds the index with the given name and returns the index in the list of
     * the index (confusing comment!).  Returns -1 if the name wasn't found.
     */
    public int findIndexWithName(String index_name) {
        int sz = indexCount();
        for (int i = 0; i < sz; ++i) {
            if (indexAt(i).getName().equals(index_name)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Finds the first index for the given column name list.  Returns -1 if an
     * index over the given composite columns was not found.
     */
    public int findIndexForColumns(String[] cols) {
        int sz = indexCount();
        for (int i = 0; i < sz; ++i) {
            String[] t_cols = indexAt(i).getColumnNames();
            if (t_cols.length == cols.length) {
                boolean passed = true;
                for (int n = 0; n < t_cols.length && passed; ++n) {
                    if (!t_cols[n].equals(cols[n])) {
                        passed = false;
                    }
                }
                if (passed) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * Returns the DataIndexDef with the given name or null if it couldn't be
     * found.
     */
    public DataIndexDef indexWithName(String index_name) {
        int i = findIndexWithName(index_name);
        if (i != -1) {
            return indexAt(i);
        } else {
            return null;
        }
    }

    /**
     * Attempts to resolve the given index name from the index in this table.
     * If 'ignore_case' is true, then we return the correct case of the index
     * name.
     */
    public String resolveIndexName(String index_name, boolean ignore_case)
            throws DatabaseException {
        int sz = indexCount();
        String found = null;
        for (int i = 0; i < sz; ++i) {
            boolean passed;
            String cur_index_name = indexAt(i).getName();
            if (ignore_case) {
                passed = cur_index_name.equalsIgnoreCase(index_name);
            } else {
                passed = cur_index_name.equals(index_name);
            }
            if (passed) {
                if (found != null) {
                    throw new DatabaseException("Ambigious index name '" +
                            index_name + "'");
                }
                found = cur_index_name;
            }
        }
        if (found == null) {
            throw new DatabaseException("Index '" + index_name + "' not found.");
        }
        return found;
    }

    /**
     * Writes this DataIndexSetDef object to the given DataOutput.
     */
    public void write(DataOutput dout) throws IOException {
        dout.writeInt(1);
        dout.writeUTF(table_name.getSchema());
        dout.writeUTF(table_name.getName());
        dout.writeInt(index_list.size());
        for (Object o : index_list) {
            ((DataIndexDef) o).write(dout);
        }
    }

    /**
     * Reads the DataIndexSetDef object from the given DataInput.
     */
    public static DataIndexSetDef read(DataInput din) throws IOException {
        int version = din.readInt();
        if (version != 1) {
            throw new IOException("Don't understand version.");
        }
        String schema = din.readUTF();
        String name = din.readUTF();
        int sz = din.readInt();
        DataIndexSetDef index_set =
                new DataIndexSetDef(new TableName(schema, name));
        for (int i = 0; i < sz; ++i) {
            index_set.addDataIndexDef(DataIndexDef.read(din));
        }

        return index_set;
    }


}

