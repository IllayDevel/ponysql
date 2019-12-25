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

package com.pony.database.global;

import java.io.*;

/**
 * This is a description of a column and the data it stores.  Specifically it
 * stores the 'type' as defined in the Types class, the 'size' if the column
 * cells may be different lengths (eg, string), the name of the column, whether
 * the column set must contain unique elements, and whether a cell may be added
 * that is null.
 *
 * @author Tobias Downer
 */

public class ColumnDescription {

//  static final long serialVersionUID = 8210197301596138014L;

    /**
     * The name of the field.
     */
    private final String name;

    /**
     * The type of the field, from the Types object.
     */
    private final int type;

    /**
     * The size of the type.  The meaning of this field changes depending on the
     * type.  For example, the size of an SQL NUMERIC represents the number of
     * digits in the value (precision).
     */
    private final int size;

    /**
     * The scale of a numerical value.  This represents the number of digits to
     * the right of the decimal point.  The number is rounded to this scale
     * in arithmatic operations.  By default, the scale is '10'
     */
    private int scale = -1;

    /**
     * The SQL type as defined in java.sql.Types.  This is required to emulate
     * the various SQL types.  The value is initialised to -9332 to indicate
     * the sql type has not be defined.
     */
    private int sql_type = -9332;

    /**
     * If true, the field may not be null.  If false, the column may contain
     * no information.  This is enforced at the parse stage when adding or
     * altering a table.
     */
    private final boolean not_null;

    /**
     * If true, the field may only contain unique values.  This is enforced at
     * the parse stage when adding or altering a table.
     */
    private boolean unique;

    /**
     * This represents the 'unique_group' that this column is in.  If two
     * columns in a table belong to the same unique_group, then the specific
     * combination of the groups columns can not exist more than once in the
     * table.
     * A value of -1 means the column does not belong to any unique group.
     */
    private int unique_group;

    /**
     * The Constructors if the type does require a size.
     */
    public ColumnDescription(String name, int type, int size, boolean not_null) {
        this.name = name;
        this.type = type;
        this.size = size;
        this.not_null = not_null;
        this.unique = false;
        this.unique_group = -1;
    }

    public ColumnDescription(String name, int type, boolean not_null) {
        this(name, type, -1, not_null);
    }

    public ColumnDescription(ColumnDescription cd) {
        this(cd.getName(), cd.getType(), cd.getSize(), cd.isNotNull());
        if (cd.isUnique()) {
            setUnique();
        }
        setUniqueGroup(cd.getUniqueGroup());
        setScale(cd.getScale());
        setSQLType(cd.getSQLType());
    }

    public ColumnDescription(String name, ColumnDescription cd) {
        this(name, cd.getType(), cd.getSize(), cd.isNotNull());
        if (cd.isUnique()) {
            setUnique();
        }
        setUniqueGroup(cd.getUniqueGroup());
        setScale(cd.getScale());
        setSQLType(cd.getSQLType());
    }

    /**
     * Sets this column to unique.
     * NOTE: This can only happen during the setup of the object.  Unpredictable
     *   results will occur otherwise.
     */
    public void setUnique() {
        this.unique = true;
    }

    /**
     * Sets the column to belong to the specified unique group in the table.
     * Setting to -1 sets the column to no unique group.
     * NOTE: This can only happen during the setup of the object.  Unpredictable
     *   results will occur otherwise.
     */
    public void setUniqueGroup(int group) {
        this.unique_group = group;
    }

    /**
     * Sets the SQL type for this ColumnDescription object.  This is only used
     * to emulate SQL types in the database.  They are mapped to the simpler
     * internal types as follows:<p>
     * <pre>
     *    DB_STRING := CHAR, VARCHAR, LONGVARCHAR
     *   DB_NUMERIC := TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, REAL,
     *                 DOUBLE, NUMERIC, DECIMAL
     *      DB_DATE := DATE, TIME, TIMESTAMP
     *   DB_BOOLEAN := BIT
     *      DB_BLOB := BINARY, VARBINARY, LONGVARBINARY
     *    DB_OBJECT := JAVA_OBJECT
     * </pre>
     */
    public void setSQLType(int sql_type) {
        this.sql_type = sql_type;
    }

    /**
     * Sets the scale of the numerical values stored.
     */
    public void setScale(int scale) {
        this.scale = scale;
    }

    /**
     * Returns the name of the field.  The field type returned should be
     * 'ZIP' or 'Address1'.  To resolve to the tables type, we must append
     * an additional 'Company.' or 'Customer.' string to the front.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns an integer representing the type of the field.  The types are
     * outlined in com.pony.database.global.Types.
     */
    public int getType() {
        return type;
    }

    /**
     * Returns true if this column is a numeric type.
     */
    public boolean isNumericType() {
        return (type == Types.DB_NUMERIC);
    }

    /**
     * Returns a value from java.sql.Type that is the SQL type defined for this
     * column.  It's possible that the column may not have had the SQL type
     * set in which case we map from the internal db type ( DB_??? ) to the
     * most logical sql type.
     */
    public int getSQLType() {
        if (sql_type == -9332) {
            // If sql type is unknown find from internal type
            if (type == Types.DB_NUMERIC) {
                return SQLTypes.NUMERIC;
            } else if (type == Types.DB_STRING) {
                return SQLTypes.LONGVARCHAR;
            } else if (type == Types.DB_BOOLEAN) {
                return SQLTypes.BIT;
            } else if (type == Types.DB_TIME) {
                return SQLTypes.TIMESTAMP;
            } else if (type == Types.DB_BLOB) {
                return SQLTypes.LONGVARBINARY;
            } else if (type == Types.DB_OBJECT) {
                return SQLTypes.JAVA_OBJECT;
            } else {
                throw new Error("Unrecognised internal type.");
            }
        }
        return sql_type;
    }

    /**
     * Returns the name (as a string) of the SQL type or null if the type is
     * not understood.
     */
    public String getSQLTypeName() {
        int type = getSQLType();
        switch (type) {
            case (SQLTypes.BIT):
                return "BIT";
            case (SQLTypes.TINYINT):
                return "TINYINT";
            case (SQLTypes.SMALLINT):
                return "SMALLINT";
            case (SQLTypes.INTEGER):
                return "INTEGER";
            case (SQLTypes.BIGINT):
                return "BIGINT";
            case (SQLTypes.FLOAT):
                return "FLOAT";
            case (SQLTypes.REAL):
                return "REAL";
            case (SQLTypes.DOUBLE):
                return "DOUBLE";
            case (SQLTypes.NUMERIC):
                return "NUMERIC";
            case (SQLTypes.DECIMAL):
                return "DECIMAL";
            case (SQLTypes.CHAR):
                return "CHAR";
            case (SQLTypes.VARCHAR):
                return "VARCHAR";
            case (SQLTypes.LONGVARCHAR):
                return "LONGVARCHAR";
            case (SQLTypes.DATE):
                return "DATE";
            case (SQLTypes.TIME):
                return "TIME";
            case (SQLTypes.TIMESTAMP):
                return "TIMESTAMP";
            case (SQLTypes.BINARY):
                return "BINARY";
            case (SQLTypes.VARBINARY):
                return "VARBINARY";
            case (SQLTypes.LONGVARBINARY):
                return "LONGVARBINARY";
            case (SQLTypes.NULL):
                return "NULL";
            case (SQLTypes.OTHER):
                return "OTHER";
            case (SQLTypes.JAVA_OBJECT):
                return "JAVA_OBJECT";
            case (SQLTypes.DISTINCT):
                return "DISTINCT";
            case (SQLTypes.STRUCT):
                return "STRUCT";
            case (SQLTypes.ARRAY):
                return "ARRAY";
            case (SQLTypes.BLOB):
                return "BLOB";
            case (SQLTypes.CLOB):
                return "CLOB";
            case (SQLTypes.REF):
                return "REF";
            case (SQLTypes.BOOLEAN):
                return "BOOLEAN";
            default:
                return null;
        }
    }


    /**
     * Returns the class of Java object for this field.
     */
    public Class<?> classType() {
        return TypeUtil.toClass(type);
//    if (type == Types.DB_STRING) {
//      return String.class;
//    }
//    else if (type == Types.DB_NUMERIC) {
//      return BigDecimal.class;
//    }
//    else if (type == Types.DB_TIME) {
//      return Date.class;
//    }
//    else if (type == Types.DB_BOOLEAN) {
//      return Boolean.class;
//    }
//    else {
//      throw new Error("Unknown type.");
//    }
    }

    /**
     * Returns the size of the given field.  This is only applicable to a few
     * of the types, ie VARCHAR.
     */
    public int getSize() {
        return size;
    }

    /**
     * If this is a number, returns the scale of the field.
     */
    public int getScale() {
        return scale;
    }

    /**
     * Determines whether the field can contain a null value or not.  Returns
     * true if it is required for the column to contain data.
     */
    public boolean isNotNull() {
        return not_null;
    }

    /**
     * Determines whether the field can contain two items that are identical.
     * Returns true if each element must be unique.
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * Returns the unique group that this column is in.  If it does not belong
     * to a unique group then the value -1 is returned.
     */
    public int getUniqueGroup() {
        return unique_group;
    }

    /**
     * Returns true if the type of the field is searchable.  Searchable means
     * that the database driver can quantify it, as in determine if a given
     * object of the same type is greater, equal or less.  We can not quantify
     * BLOB types.
     */
    public boolean isQuantifiable() {
        return type != Types.DB_BLOB &&
                type != Types.DB_OBJECT;
    }

    /**
     * The 'equals' method, used to determine equality between column
     * descriptions.
     */
    public boolean equals(Object ob) {
        ColumnDescription cd = (ColumnDescription) ob;
        return (name.equals(cd.name) &&
                type == cd.type &&
                size == cd.size &&
                not_null == cd.not_null &&
                unique == cd.unique &&
                unique_group == cd.unique_group);
    }

    /**
     * Writes this ColumnDescription to the given DataOutputStream.
     * ( Remember to flush output stream )
     */
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeUTF(name);
        out.writeInt(type);
        out.writeInt(size);
        out.writeBoolean(not_null);
        out.writeBoolean(unique);
        out.writeInt(unique_group);
        out.writeInt(sql_type);
        out.writeInt(scale);
    }

    /**
     * Reads a ColumnDescription from the given DataInputStream and returns
     * a new instance of it.
     */
    public static ColumnDescription readFrom(DataInputStream in)
            throws IOException {
        String name = in.readUTF();
        int type = in.readInt();
        int size = in.readInt();
        boolean not_null = in.readBoolean();
        boolean unique = in.readBoolean();
        int unique_group = in.readInt();

        ColumnDescription col_desc = new ColumnDescription(name,
                type, size, not_null);
        if (unique) col_desc.setUnique();
        col_desc.setUniqueGroup(unique_group);
        col_desc.setSQLType(in.readInt());
        col_desc.setScale(in.readInt());

        return col_desc;
    }

}
