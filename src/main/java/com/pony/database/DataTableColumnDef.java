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

import java.io.*;

import com.pony.database.global.ColumnDescription;
import com.pony.database.global.SQLTypes;

/**
 * All the information regarding a column in a table.
 *
 * @author Tobias Downer
 */

public class DataTableColumnDef {

    /**
     * A string that contains some constraints.  This string contains
     * information about whether the column is not null, unique, primary key,
     * etc.
     */
    private final byte[] constraints_format = new byte[16];

    /**
     * The name of the column.
     */
    private String name;

    /**
     * The sql column type (as defined in java.sql.Types).
     */
    private int sql_type;

    /**
     * The actual column type in the database (as defined in
     * com.pony.database.global.Types).
     */
    private int db_type;

    /**
     * The size of the data.
     */
    private int size;

    /**
     * The scale of the data.
     */
    private int scale;

    /**
     * The locale string if this column represents a string.  If this is an
     * empty string, the column has no locale (the string is collated
     * lexicographically).
     */
    private String locale_str = "";

    /**
     * The locale Collator strength if this column represents a string.  The
     * value here is taken from java.text.Collator.
     */
    private int str_strength;

    /**
     * The locale Collator decomposition if this column represents a string.  The
     * value here is taken from java.text.Collator.
     */
    private int str_decomposition;

    /**
     * The default expression string.
     */
    private String default_expression_string;

//  /**
//   * The expression that is executed to set the default value.
//   */
//  private Expression default_exp;

    /**
     * If this is a foreign key, the table.column that this foreign key
     * refers to.
     * @deprecated
     */
    private String foreign_key = "";

    /**
     * The type of index to use on this column.
     */
    private String index_desc = "";

    /**
     * If this is a Java Object column, this is a constraint that the object
     * must be derived from to be added to this column.  If not specified,
     * it defaults to 'java.lang.Object'.
     */
    private String class_constraint = "";

    /**
     * The constraining Class object itself.
     */
    private Class constraining_class;

    /**
     * The TType object for this column.
     */
    public TType type;


    /**
     * Constructs the column definition.
     */
    public DataTableColumnDef() {
    }

    /**
     * Creates a copy of the given column definition.
     */
    public DataTableColumnDef(DataTableColumnDef column_def) {
        System.arraycopy(column_def.constraints_format, 0,
                constraints_format, 0, constraints_format.length);
        name = column_def.name;
        sql_type = column_def.sql_type;
        db_type = column_def.db_type;
        size = column_def.size;
        scale = column_def.scale;
        locale_str = column_def.locale_str;
        str_strength = column_def.str_strength;
        str_decomposition = column_def.str_decomposition;
        if (column_def.default_expression_string != null) {
            default_expression_string = column_def.default_expression_string;
//      default_exp = new Expression(column_def.default_exp);
        }
        foreign_key = column_def.foreign_key;
        index_desc = column_def.index_desc;
        class_constraint = column_def.class_constraint;
        type = column_def.type;
    }

    // ---------- Set methods ----------

    public void setName(String name) {
        this.name = name;
    }

    public void setNotNull(boolean status) {
        constraints_format[0] = (byte) (status ? 1 : 0);
    }

//  public void setUnique(boolean status) {
//    constraints_format[1] = (byte) (status ? 1 : 0);
//  }
//
//  public void setPrimaryKey(boolean status) {
//    constraints_format[2] = (byte) (status ? 1 : 0);
//  }

    public void setSQLType(int sql_type) {
        this.sql_type = sql_type;
        if (sql_type == SQLTypes.BIT ||
                sql_type == SQLTypes.BOOLEAN) {
            db_type = com.pony.database.global.Types.DB_BOOLEAN;
        } else if (sql_type == SQLTypes.TINYINT ||
                sql_type == SQLTypes.SMALLINT ||
                sql_type == SQLTypes.INTEGER ||
                sql_type == SQLTypes.BIGINT ||
                sql_type == SQLTypes.FLOAT ||
                sql_type == SQLTypes.REAL ||
                sql_type == SQLTypes.DOUBLE ||
                sql_type == SQLTypes.NUMERIC ||
                sql_type == SQLTypes.DECIMAL) {
            db_type = com.pony.database.global.Types.DB_NUMERIC;
        } else if (sql_type == SQLTypes.CHAR ||
                sql_type == SQLTypes.VARCHAR ||
                sql_type == SQLTypes.LONGVARCHAR) {
            db_type = com.pony.database.global.Types.DB_STRING;
        } else if (sql_type == SQLTypes.DATE ||
                sql_type == SQLTypes.TIME ||
                sql_type == SQLTypes.TIMESTAMP) {
            db_type = com.pony.database.global.Types.DB_TIME;
        } else if (sql_type == SQLTypes.BINARY ||
                sql_type == SQLTypes.VARBINARY ||
                sql_type == SQLTypes.LONGVARBINARY) {
            db_type = com.pony.database.global.Types.DB_BLOB;
        } else if (sql_type == SQLTypes.JAVA_OBJECT) {
            db_type = com.pony.database.global.Types.DB_OBJECT;
        } else {
            db_type = com.pony.database.global.Types.DB_UNKNOWN;
        }
    }

    public void setDBType(int db_type) {
        this.db_type = db_type;
        if (db_type == com.pony.database.global.Types.DB_NUMERIC) {
            sql_type = SQLTypes.NUMERIC;
        } else if (db_type == com.pony.database.global.Types.DB_STRING) {
            sql_type = SQLTypes.LONGVARCHAR;
        } else if (db_type == com.pony.database.global.Types.DB_BOOLEAN) {
            sql_type = SQLTypes.BIT;
        } else if (db_type == com.pony.database.global.Types.DB_TIME) {
            sql_type = SQLTypes.TIMESTAMP;
        } else if (db_type == com.pony.database.global.Types.DB_BLOB) {
            sql_type = SQLTypes.LONGVARBINARY;
        } else if (db_type == com.pony.database.global.Types.DB_OBJECT) {
            sql_type = SQLTypes.JAVA_OBJECT;
        } else {
            throw new Error("Unrecognised internal type.");
        }
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public void setStringLocale(String locale_str,
                                int strength, int decomposition) {
        // Sets this column to be of the given locale.  For example, the string
        // "frFR" denotes french/france.  See com/pony/database/TStringType.java
        // for more information.
        if (locale_str == null) {
            this.locale_str = "";
        } else {
            this.locale_str = locale_str;
            this.str_strength = strength;
            this.str_decomposition = decomposition;
        }
    }

    public void setDefaultExpression(Expression expression) {
        this.default_expression_string = expression.text().toString();
    }

    /**
     * @deprecated
     */
    public void setForeignKey(String foreign_key) {
        this.foreign_key = foreign_key;
    }

    /**
     * Sets the indexing scheme for this column.  Either 'InsertSearch' or
     * 'BlindSearch'.  If not set, then default to insert search.
     */
    public void setIndexScheme(String index_scheme) {
        index_desc = index_scheme;
    }

    /**
     * If this column represents a Java object, this must be a class the object
     * is derived from to be added to this column.
     */
    public void setClassConstraint(String class_constraint) {
        this.class_constraint = class_constraint;
        try {
            // Denotes an array
            if (class_constraint.endsWith("[]")) {
                String array_class =
                        class_constraint.substring(0, class_constraint.length() - 2);
                Class ac;
                // Arrays of primitive types,
                switch (array_class) {
                    case "boolean":
                        ac = boolean.class;
                        break;
                    case "byte":
                        ac = byte.class;
                        break;
                    case "char":
                        ac = char.class;
                        break;
                    case "short":
                        ac = short.class;
                        break;
                    case "int":
                        ac = int.class;
                        break;
                    case "long":
                        ac = long.class;
                        break;
                    case "float":
                        ac = float.class;
                        break;
                    case "double":
                        ac = double.class;
                        break;
                    default:
                        // Otherwise a standard array.
                        ac = Class.forName(array_class);
                        break;
                }
                // Make it into an array
                constraining_class =
                        java.lang.reflect.Array.newInstance(ac, 0).getClass();
            } else {
                // Not an array
                constraining_class = Class.forName(class_constraint);
            }
        } catch (ClassNotFoundException e) {
            throw new Error("Unable to resolve class: " + class_constraint);
        }
    }

    /**
     * Sets this DataTableColumnDef object up from information in the TType
     * object.  This is useful when we need to create a DataTableColumnDef object
     * to store information based on nothing more than a TType object.  This
     * comes in useful for purely functional tables.
     */
    public void setFromTType(TType type) {
        setSQLType(type.getSQLType());
        if (type instanceof TStringType) {
            TStringType str_type = (TStringType) type;
            setSize(str_type.getMaximumSize());
            setStringLocale(str_type.getLocaleString(),
                    str_type.getStrength(), str_type.getDecomposition());
        } else if (type instanceof TNumericType) {
            TNumericType num_type = (TNumericType) type;
            setSize(num_type.getSize());
            setScale(num_type.getScale());
        } else if (type instanceof TBooleanType) {
            // Nothing necessary for booleans
//      TBooleanType bool_type = (TBooleanType) type;
        } else if (type instanceof TDateType) {
            // Nothing necessary for dates
//      TDateType date_type = (TDateType) type;
        } else if (type instanceof TNullType) {
            // Nothing necessary for nulls
        } else if (type instanceof TBinaryType) {
            TBinaryType binary_type = (TBinaryType) type;
            setSize(binary_type.getMaximumSize());
        } else if (type instanceof TJavaObjectType) {
            TJavaObjectType java_object_type = (TJavaObjectType) type;
            setClassConstraint(java_object_type.getJavaClassTypeString());
        } else {
            throw new Error("Don't know how to handle this type: " +
                    type.getClass());
        }
        this.type = type;

    }

    /**
     * Initializes the TType information for a column.  This should be called
     * at the last part of a DataTableColumnDef setup.
     */
    public void initTTypeInfo() {
        if (type == null) {
            type = createTTypeFor(getSQLType(), getSize(), getScale(),
                    getLocaleString(), getStrength(), getDecomposition(),
                    getClassConstraint());
        }
    }


    // ---------- Get methods ----------

    public String getName() {
        return name;
    }

    public boolean isNotNull() {
        return constraints_format[0] != 0;
    }

    public int getSQLType() {
        return sql_type;
    }

    /**
     * Returns the type as a String.
     */
    public String getSQLTypeString() {
        return sqlTypeToString(getSQLType());
    }

    /**
     * Returns the type as a String.
     */
    public String getDBTypeString() {
        switch (getDBType()) {
            case com.pony.database.global.Types.DB_NUMERIC:
                return "DB_NUMERIC";
            case com.pony.database.global.Types.DB_STRING:
                return "DB_STRING";
            case com.pony.database.global.Types.DB_BOOLEAN:
                return "DB_BOOLEAN";
            case com.pony.database.global.Types.DB_TIME:
                return "DB_TIME";
            case com.pony.database.global.Types.DB_BLOB:
                return "DB_BLOB";
            case com.pony.database.global.Types.DB_OBJECT:
                return "DB_OBJECT";
            default:
                return "UNKNOWN(" + getDBType() + ")";
        }
    }

    /**
     * Returns the Class of Java object that represents this column.
     */
    public Class classType() {
        return com.pony.database.global.TypeUtil.toClass(getDBType());
    }

    public int getDBType() {
        return db_type;
    }

    public int getSize() {
        return size;
    }

    public int getScale() {
        return scale;
    }

    public String getLocaleString() {
        return locale_str;
    }

    public int getStrength() {
        return str_strength;
    }

    public int getDecomposition() {
        return str_decomposition;
    }

    public Expression getDefaultExpression(TransactionSystem system) {
        if (default_expression_string == null) {
            return null;
        }
        Expression exp = Expression.parse(default_expression_string);
        return exp;
    }

    public String getDefaultExpressionString() {
        return default_expression_string;
    }

    /**
     * @deprecated
     */
    public String getForeignKey() {
        return foreign_key;
    }

    /**
     * Returns the name of the scheme we use to index this column.  It will
     * be either 'InsertSearch' or 'BlindSearch'.
     */
    public String getIndexScheme() {
        if (index_desc.equals("")) {
            return "InsertSearch";
        }
        return index_desc;
    }

    /**
     * Returns true if this type of column is able to be indexed.
     */
    public boolean isIndexableType() {
        return getDBType() != com.pony.database.global.Types.DB_BLOB &&
                getDBType() != com.pony.database.global.Types.DB_OBJECT;
    }

    /**
     * If this column represents a Java Object, this returns the name of the
     * class the objects stored in the column must be derived from.
     */
    public String getClassConstraint() {
        return class_constraint;
    }

    /**
     * If this column represents a Java Object, this returns the class object
     * that is the constraining class for the column.
     */
    public Class getClassConstraintAsClass() {
        return constraining_class;
    }

    /**
     * Returns the TType for this column.
     */
    public TType getTType() {
        if (type == null) {
            throw new Error("'type' variable was not set.");
        }
        return type;
    }

//  /**
//   * Returns this column as a TableField object.
//   * 
//   * @deprecated TableField shouldn't be used anymore
//   */
//  public TableField tableFieldValue() {
//    TableField field =
//               new TableField(getName(), getDBType(), getSize(), isNotNull());
////    if (isUnique()) {
////      field.setUnique();
////    }
//    field.setScale(getScale());
//    field.setSQLType(getSQLType());
//
//    return field;
//  }

    /**
     * Returns this column as a ColumnDescription object and gives the column
     * description the given name.
     */
    public ColumnDescription columnDescriptionValue(String column_name) {
        ColumnDescription field =
                new ColumnDescription(column_name, getDBType(), getSize(), isNotNull());
        field.setScale(getScale());
        field.setSQLType(getSQLType());

        return field;
    }

    /**
     * Dumps information about this object to the PrintStream.
     */
    public void dump(PrintStream out) {
        out.print(getName());
        out.print("(");
        out.print(getSQLTypeString());
        out.print(")");
    }

    // ---------- For compatibility with older versions only --------
    // These are made available only because we need to convert from the
    // pre table constraint versions.

    boolean compatIsUnique() {
        return constraints_format[1] != 0;
    }

    boolean compatIsPrimaryKey() {
        return constraints_format[2] != 0;
    }

    // ---------- Convenient static methods ----------

    /**
     * Returns a string that represents the given SQLType enumeration passed
     * to it.  For example, pass SQLTypes.BIT and it returns the string "BIT"
     */
    public static String sqlTypeToString(int sql_type) {
        switch (sql_type) {
            case SQLTypes.BIT:
                return "BIT";
            case SQLTypes.TINYINT:
                return "TINYINT";
            case SQLTypes.SMALLINT:
                return "SMALLINT";
            case SQLTypes.INTEGER:
                return "INTEGER";
            case SQLTypes.BIGINT:
                return "BIGINT";
            case SQLTypes.FLOAT:
                return "FLOAT";
            case SQLTypes.REAL:
                return "REAL";
            case SQLTypes.DOUBLE:
                return "DOUBLE";
            case SQLTypes.NUMERIC:
                return "NUMERIC";
            case SQLTypes.DECIMAL:
                return "DECIMAL";
            case SQLTypes.CHAR:
                return "CHAR";
            case SQLTypes.VARCHAR:
                return "VARCHAR";
            case SQLTypes.LONGVARCHAR:
                return "LONGVARCHAR";
            case SQLTypes.DATE:
                return "DATE";
            case SQLTypes.TIME:
                return "TIME";
            case SQLTypes.TIMESTAMP:
                return "TIMESTAMP";
            case SQLTypes.BINARY:
                return "BINARY";
            case SQLTypes.VARBINARY:
                return "VARBINARY";
            case SQLTypes.LONGVARBINARY:
                return "LONGVARBINARY";
            case SQLTypes.JAVA_OBJECT:
                return "JAVA_OBJECT";
            case SQLTypes.NULL:
                return "NULL";
            case SQLTypes.BOOLEAN:
                return "BOOLEAN";
            default:
                return "UNKNOWN(" + sql_type + ")";
        }
    }

    /**
     * Returns a TType object for a column with the given type information.  The
     * type information is the sql_type, the size and the scale of the type.
     */
    static TType createTTypeFor(int sql_type, int size, int scale,
                                String locale, int str_strength, int str_decomposition,
                                String java_class) {
        switch (sql_type) {
            case (SQLTypes.BIT):
            case (SQLTypes.BOOLEAN):
                return TType.BOOLEAN_TYPE;

            case (SQLTypes.TINYINT):
            case (SQLTypes.SMALLINT):
            case (SQLTypes.INTEGER):
            case (SQLTypes.BIGINT):
            case (SQLTypes.FLOAT):
            case (SQLTypes.REAL):
            case (SQLTypes.DOUBLE):
            case (SQLTypes.NUMERIC):
            case (SQLTypes.DECIMAL):
                return new TNumericType(sql_type, size, scale);

            case (SQLTypes.CHAR):
            case (SQLTypes.VARCHAR):
            case (SQLTypes.LONGVARCHAR):
            case (SQLTypes.CLOB):
                return new TStringType(sql_type, size, locale,
                        str_strength, str_decomposition);

            case (SQLTypes.DATE):
            case (SQLTypes.TIME):
            case (SQLTypes.TIMESTAMP):
                return new TDateType(sql_type);

            case (SQLTypes.BINARY):
            case (SQLTypes.VARBINARY):
            case (SQLTypes.LONGVARBINARY):
            case (SQLTypes.BLOB):
                return new TBinaryType(sql_type, size);

            case (SQLTypes.JAVA_OBJECT):
                return new TJavaObjectType(java_class);

            case (SQLTypes.ARRAY):
                return TType.ARRAY_TYPE;

            case (SQLTypes.NULL):
                return TType.NULL_TYPE;

            default:
                throw new Error("SQL type not recognized.");
        }
    }

    /**
     * Convenience helper - creates a DataTableColumnDef that
     * holds a numeric value.
     */
    public static DataTableColumnDef createNumericColumn(String name) {
        DataTableColumnDef column = new DataTableColumnDef();
        column.setName(name);
        column.setSQLType(java.sql.Types.NUMERIC);
        column.initTTypeInfo();
        return column;
    }

    /**
     * Convenience helper - creates a DataTableColumnDef that
     * holds a boolean value.
     */
    public static DataTableColumnDef createBooleanColumn(String name) {
        DataTableColumnDef column = new DataTableColumnDef();
        column.setName(name);
        column.setSQLType(java.sql.Types.BIT);
        column.initTTypeInfo();
        return column;
    }

    /**
     * Convenience helper - creates a DataTableColumnDef that
     * holds a string value.
     */
    public static DataTableColumnDef createStringColumn(String name) {
        DataTableColumnDef column = new DataTableColumnDef();
        column.setName(name);
        column.setSQLType(java.sql.Types.VARCHAR);
        column.setSize(Integer.MAX_VALUE);
        column.initTTypeInfo();
        return column;
    }

    /**
     * Convenience helper - creates a DataTableColumnDef that
     * holds a binary value.
     */
    public static DataTableColumnDef createBinaryColumn(String name) {
        DataTableColumnDef column = new DataTableColumnDef();
        column.setName(name);
        column.setSQLType(java.sql.Types.LONGVARBINARY);
        column.setSize(Integer.MAX_VALUE);
        column.setIndexScheme("BlindSearch");
        column.initTTypeInfo();
        return column;
    }


    // ---------- IO Methods ----------

    /**
     * Writes this column information out to a DataOutputStream.
     */
    void write(DataOutput out) throws IOException {

        out.writeInt(2);    // The version

        out.writeUTF(name);
        out.writeInt(constraints_format.length);
        out.write(constraints_format);
        out.writeInt(sql_type);
        out.writeInt(db_type);
        out.writeInt(size);
        out.writeInt(scale);

        if (default_expression_string != null) {
            out.writeBoolean(true);
            out.writeUTF(default_expression_string);
            //new String(default_exp.text().toString()));
        } else {
            out.writeBoolean(false);
        }

        out.writeUTF(foreign_key);
        out.writeUTF(index_desc);
        out.writeUTF(class_constraint);    // Introduced in version 2.

        // Format the 'other' string
        StringBuffer other = new StringBuffer();
        other.append("|");
        other.append(locale_str);
        other.append("|");
        other.append(str_strength);
        other.append("|");
        other.append(str_decomposition);
        other.append("|");
        // And write it
        out.writeUTF(new String(other));
    }

    /**
     * Reads this column from a DataInputStream.
     */
    static DataTableColumnDef read(DataInput in) throws IOException {

        int ver = in.readInt();

        DataTableColumnDef cd = new DataTableColumnDef();
        cd.name = in.readUTF();
        int len = in.readInt();
        in.readFully(cd.constraints_format, 0, len);
        cd.sql_type = in.readInt();
        cd.db_type = in.readInt();
        cd.size = in.readInt();
        cd.scale = in.readInt();

        boolean b = in.readBoolean();
        if (b) {
            cd.default_expression_string = in.readUTF();
//      cd.default_exp = Expression.parse(in.readUTF());
        }
        cd.foreign_key = in.readUTF();
        cd.index_desc = in.readUTF();
        if (ver > 1) {
            String cc = in.readUTF();
            if (!cc.equals("")) {
                cd.setClassConstraint(cc);
            }
        } else {
            cd.class_constraint = "";
        }

        // Parse the 'other' string
        String other = in.readUTF();
        if (other.length() > 0) {
            if (other.startsWith("|")) {
                // Read the string locale, collation strength and disposition
                int cur_i = 1;
                int next_break = other.indexOf("|", cur_i);
                cd.locale_str = other.substring(cur_i, next_break);

                cur_i = next_break + 1;
                next_break = other.indexOf("|", cur_i);
                cd.str_strength =
                        Integer.parseInt(other.substring(cur_i, next_break));

                cur_i = next_break + 1;
                next_break = other.indexOf("|", cur_i);
                cd.str_decomposition =
                        Integer.parseInt(other.substring(cur_i, next_break));

            } else {
                throw new Error("Incorrectly formatted DataTableColumnDef data.");
            }
        }

        cd.initTTypeInfo();

        return cd;
    }

}
