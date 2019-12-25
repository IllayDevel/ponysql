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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents index meta-information on a table.  This information is part of
 * DataIndexSetDef and is stored with the contents of a table.
 *
 * @author Tobias Downer
 */

public class DataIndexDef {

    /**
     * The name of this index.
     */
    private String index_name;

    /**
     * The list of column name that this index represents.  For example, if this
     * is a composite primary key, this would contain each column name in the
     * primary key.
     */
    private String[] column_names;

    /**
     * Returns the index set pointer of this index.  This value is used when
     * requesting the index from an IndexSet.
     */
    private int index_pointer;

    /**
     * The type of Index this is.  Currently only 'BLIST' is supported.
     */
    private String index_type;

    /**
     * True if this index may only contain unique values.
     */
    private boolean unique;

    /**
     * Constructor.
     */
    public DataIndexDef(String index_name, String[] column_names,
                        int index_pointer, String index_type, boolean unique) {

        this.index_name = index_name;
        this.column_names = (String[]) column_names.clone();
        this.index_pointer = index_pointer;
        this.index_type = index_type;
        this.unique = unique;

    }

    public DataIndexDef(DataIndexDef def) {
        this(def.index_name, def.column_names, def.index_pointer, def.index_type,
                def.unique);
    }

    /**
     * Returns the name of this index.
     */
    public String getName() {
        return index_name;
    }

    /**
     * Returns the column names that make up this index.
     */
    public String[] getColumnNames() {
        return column_names;
    }

    /**
     * Returns the pointer to the index in the IndexSet.
     */
    public int getPointer() {
        return index_pointer;
    }

    /**
     * Returns a String that describes the type of index this is.
     */
    public String getType() {
        return index_type;
    }

    /**
     * Returns true if this is a unique index.
     */
    public boolean isUniqueIndex() {
        return unique;
    }

    /**
     * Writes this object to the given DataOutputStream.
     */
    public void write(DataOutput dout) throws IOException {
        dout.writeInt(1);
        dout.writeUTF(index_name);
        dout.writeInt(column_names.length);
        for (int i = 0; i < column_names.length; ++i) {
            dout.writeUTF(column_names[i]);
        }
        dout.writeInt(index_pointer);
        dout.writeUTF(index_type);
        dout.writeBoolean(unique);
    }

    /**
     * Reads a DataIndexDef from the given DataInput object.
     */
    public static DataIndexDef read(DataInput din) throws IOException {
        int version = din.readInt();
        if (version != 1) {
            throw new IOException("Don't understand version.");
        }
        String index_name = din.readUTF();
        int sz = din.readInt();
        String[] cols = new String[sz];
        for (int i = 0; i < sz; ++i) {
            cols[i] = din.readUTF();
        }
        int index_pointer = din.readInt();
        String index_type = din.readUTF();
        boolean unique = din.readBoolean();

        return new DataIndexDef(index_name, cols,
                index_pointer, index_type, unique);
    }

}

