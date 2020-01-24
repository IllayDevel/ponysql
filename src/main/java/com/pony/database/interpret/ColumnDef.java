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
import com.pony.database.sql.ParseException;
import com.pony.database.sql.SQLConstants;
import com.pony.database.sql.Token;

/**
 * Represents a column definition (description).
 *
 * @author Tobias Downer
 */

public final class ColumnDef
        implements java.io.Serializable, StatementTreeObject, Cloneable {

    static final long serialVersionUID = 8347617136528650961L;

//  DataTableColumnDef col;

    String name;

//  int sql_type;
//  int size;
//  int scale;
//  String class_constraint;
//
//  String locale_str;
//  int strength;
//  int decomposition;

    TType type;
    String index_str;

    Expression default_expression;
    Expression original_default_expression;

    private boolean not_null = false;
    private boolean primary_key = false;
    private boolean unique = false;

    public ColumnDef() {
//    col = new DataTableColumnDef();
    }

    /**
     * Returns true if this column has a primary key constraint set on it.
     */
    public boolean isPrimaryKey() {
        return primary_key;
    }

    /**
     * Returns true if this column has the unique constraint set for it.
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * Returns true if this column has the not null constraint set for it.
     */
    public boolean isNotNull() {
        return not_null;
    }

    /**
     * Sets the name of the column.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Adds a constraint to this column.
     */
    public void addConstraint(String constraint) {
        switch (constraint) {
            case "NOT NULL":
                not_null = true;
//      col.setNotNull(true);
                break;
            case "NULL":
                not_null = false;
//      col.setNotNull(false);
                break;
            case "PRIMARY":
                primary_key = true;
                break;
            case "UNIQUE":
                unique = true;
                break;
            default:
                throw new RuntimeException("Unknown constraint: " + constraint);
        }
    }

    /**
     * Sets the type of data of this column.
     */
    public void setDataType(TType type) {
        this.type = type;
    }

//  /**
//   * Sets the type of data this column is.
//   */
//  public void setDataType(String type, int size, int scale)
//                                                      throws ParseException {
//    int data_type;
//
//    String ltype = type.toLowerCase();
//    if (ltype.equals("bit") || ltype.equals("boolean")) {
//      data_type = SQLTypes.BIT;
//      if (size != -1 || scale != -1) {
//        throw new ParseException("size/scale for bit.");
//      }
//    }
//    else if (ltype.equals("tinyint")) {
//      data_type = SQLTypes.TINYINT;
//    }
//    else if (ltype.equals("smallint")) {
//      data_type = SQLTypes.SMALLINT;
//    }
//    else if (ltype.equals("integer") || ltype.equals("int")) {
//      data_type = SQLTypes.INTEGER;
//    }
//    else if (ltype.equals("bigint")) {
//      data_type = SQLTypes.BIGINT;
//    }
//    else if (ltype.equals("float")) {
//      data_type = SQLTypes.FLOAT;
//    }
//    else if (ltype.equals("real")) {
//      data_type = SQLTypes.REAL;
//    }
//    else if (ltype.equals("double")) {
//      data_type = SQLTypes.DOUBLE;
//    }
//    else if (ltype.equals("numeric")) {
//      data_type = SQLTypes.NUMERIC;
//    }
//    else if (ltype.equals("decimal")) {
//      data_type = SQLTypes.DECIMAL;
//    }
//    else if (ltype.equals("char")) {
//      data_type = SQLTypes.CHAR;
//      if (scale != -1) {
//        throw new ParseException("scale for char.");
//      }
//      if (size == -1) {
//        size = 1;
//      }
//    }
//    else if (ltype.equals("varchar")) {
//      data_type = SQLTypes.VARCHAR;
//      if (scale != -1) {
//        throw new ParseException("scale for varchar.");
//      }
//      if (size == -1) size = Integer.MAX_VALUE;
//    }
//    else if (ltype.equals("longvarchar") || ltype.equals("string") ||
//             ltype.equals("text") ) {
//      data_type = SQLTypes.LONGVARCHAR;
//      if (scale != -1) {
//        throw new ParseException("scale for longvarchar.");
//      }
//      if (size == -1) size = Integer.MAX_VALUE;
//    }
//    else if (ltype.equals("date")) {
//      data_type = SQLTypes.DATE;
//      if (size != -1 || scale != -1) {
//        throw new ParseException("size/scale for date.");
//      }
//    }
//    else if (ltype.equals("time")) {
//      data_type = SQLTypes.TIME;
//      if (size != -1 || scale != -1) {
//        throw new ParseException("size/scale for time.");
//      }
//    }
//    else if (ltype.equals("timestamp")) {
//      data_type = SQLTypes.TIMESTAMP;
//      if (size != -1 || scale != -1) {
//        throw new ParseException("size/scale for timestamp.");
//      }
//    }
//    else if (ltype.equals("binary")) {
//      data_type = SQLTypes.BINARY;
//      if (scale != -1) {
//        throw new ParseException("scale for binary.");
//      }
//      if (size == -1) {
//        size = Integer.MAX_VALUE;
//      }
//    }
//    else if (ltype.equals("varbinary")) {
//      data_type = SQLTypes.VARBINARY;
//      if (scale != -1) {
//        throw new ParseException("scale for varbinary.");
//      }
//      if (size == -1) {
//        size = Integer.MAX_VALUE;
//      }
//    }
//    else if (ltype.equals("longvarbinary") ||
//             ltype.equals("blob")) {
//      data_type = SQLTypes.LONGVARBINARY;
//      if (scale != -1) {
//        throw new ParseException("scale for longvarbinary.");
//      }
//      if (size == -1) {
//        size = Integer.MAX_VALUE;
//      }
//    }
//    else {
//      throw new ParseException("Unknown type: " + ltype);
//    }
//
//    this.sql_type = data_type;
//    this.size = size;
//    this.scale = scale;
//
//  }
//
//  /**
//   * Sets the column definition for a java object type.
//   */
//  public void setDataType(String type, Token class_ref) {
//    if (!type.equals("JAVA_OBJECT")) {
//      throw new Error("setDataType called with incorrect type.");
//    }
//
//    // Default class constraint is 'java.lang.Object'
//    String class_constraint = "java.lang.Object";
//    if (class_ref != null) {
//      class_constraint = class_ref.image;
//    }
//
//    this.sql_type = SQLTypes.JAVA_OBJECT;
//    this.size = -1;
//    this.scale = -1;
//    this.class_constraint = class_constraint;
//
//  }
//
//  /**
//   * Sets the locale, and collate strength and decomposition of this string
//   * column.  If strength or decomposition are -1 then use the default
//   * strength and decomposition levels.
//   */
//  public void setCollateType(String locale_str,
//                             int strength, int decomposition) {
//    this.locale_str = locale_str;
//    this.strength = strength;
//    this.decomposition = decomposition;
//  }

    /**
     * Sets the indexing.
     */
    public void setIndex(Token t) throws ParseException {
        if (t.kind == SQLConstants.INDEX_NONE) {
            index_str = "BlindSearch";
//      col.setIndexScheme("BlindSearch");
        } else if (t.kind == SQLConstants.INDEX_BLIST) {
            index_str = "InsertSearch";
//      col.setIndexScheme("InsertSearch");
        } else {
            throw new ParseException("Unrecognized indexing scheme.");
        }
    }

    /**
     * Sets the default expression (this is used to make a new constraint).
     */
    public void setDefaultExpression(Expression exp) {
        default_expression = exp;
        try {
            original_default_expression = (Expression) exp.clone();
        } catch (CloneNotSupportedException e) {
            throw new Error(e.getMessage());
        }
    }


    // Implemented from StatementTreeObject
    public void prepareExpressions(ExpressionPreparer preparer)
            throws DatabaseException {
        if (default_expression != null) {
            default_expression.prepare(preparer);
        }
    }

    public Object clone() throws CloneNotSupportedException {
        ColumnDef v = (ColumnDef) super.clone();
        if (default_expression != null) {
            v.default_expression = (Expression) default_expression.clone();
        }
        return v;
    }

}
