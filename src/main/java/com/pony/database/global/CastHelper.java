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

import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.text.DateFormat;

import com.pony.util.BigNumber;

import java.util.Date;

/**
 * Various utility methods for helping to cast a Java object to a type that
 * is conformant to an SQL type.
 *
 * @author Tobias Downer
 */

public class CastHelper {

    /**
     * A couple of standard BigNumber statics.
     */
    private static final BigNumber BD_ZERO = BigNumber.fromLong(0);
    private static final BigNumber BD_ONE = BigNumber.fromLong(1);

    /**
     * Date, Time and Timestamp parser/formatters
     */
    private static final DateFormat[] date_format_sql;
    private static final DateFormat[] time_format_sql;
    private static final DateFormat[] ts_format_sql;

    static {
        // The SQL time/date formatters
        date_format_sql = new DateFormat[1];
        date_format_sql[0] = new SimpleDateFormat("yyyy-MM-dd");

        time_format_sql = new DateFormat[4];
        time_format_sql[0] = new SimpleDateFormat("HH:mm:ss.S z");
        time_format_sql[1] = new SimpleDateFormat("HH:mm:ss.S");
        time_format_sql[2] = new SimpleDateFormat("HH:mm:ss z");
        time_format_sql[3] = new SimpleDateFormat("HH:mm:ss");

        ts_format_sql = new DateFormat[4];
        ts_format_sql[0] = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S z");
        ts_format_sql[1] = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        ts_format_sql[2] = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        ts_format_sql[3] = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }


    /**
     * Converts the given object to an SQL JAVA_OBJECT type by serializing
     * the object.
     */
    private static Object toJavaObject(Object ob) {
        try {
            return ObjectTranslator.serialize(ob);
        } catch (Throwable e) {
            throw new Error("Can't serialize object " + ob.getClass());
        }
    }

    /**
     * Formats the date object as a standard SQL string.
     */
    private static String formatDateAsString(Date d) {
        synchronized (ts_format_sql) {
            // ISSUE: We have to assume the date is a time stamp because we don't
            //   know if the date object represents an SQL DATE, TIMESTAMP or TIME.
            return ts_format_sql[1].format(d);
        }
    }

    /**
     * Returns the given string padded or truncated to the given size.  If size
     * is -1 then the size doesn't matter.
     */
    private static String paddedString(String str, int size) {
        if (size == -1) {
            return str;
        }
        int dif = size - str.length();
        if (dif > 0) {
            StringBuffer buf = new StringBuffer(str);
            for (int n = 0; n < dif; ++n) {
                buf.append(' ');
            }
            return new String(buf);
        } else if (dif < 0) {
            return str.substring(0, size);
        }
        return str;
    }

    /**
     * Returns the given long value as a date object.
     */
    private static Date toDate(long time) {
        return new Date(time);
    }

    /**
     * Converts the given string to a BigNumber.  Returns 0 if the cast fails.
     */
    private static BigNumber toBigNumber(String str) {
        try {
            return BigNumber.fromString(str);
        } catch (Throwable e) {
            return BD_ZERO;
        }
    }

    /**
     * Helper that generates an appropriate error message for a date format error.
     */
    private static String dateErrorString(String msg, DateFormat[] df) {
        String pattern = "";
        if (df[0] instanceof SimpleDateFormat) {
            SimpleDateFormat sdf = (SimpleDateFormat) df[0];
            pattern = "(" + sdf.toPattern() + ")";
        }
        return msg + pattern;
    }

    /**
     * Parses a String as an SQL date.
     */
    public static Date toDate(String str) {
        synchronized (date_format_sql) {
            for (DateFormat dateFormat : date_format_sql) {
                try {
                    return dateFormat.parse(str);
                } catch (ParseException e) {
                }
            }
            throw new RuntimeException(
                    dateErrorString("Unable to parse string as a date ",
                            date_format_sql));
        }
    }

    /**
     * Parses a String as an SQL time.
     */
    public static Date toTime(String str) {
        synchronized (time_format_sql) {
            for (DateFormat dateFormat : time_format_sql) {
                try {
                    return dateFormat.parse(str);
                } catch (ParseException e) {
                }
            }
            throw new RuntimeException(
                    dateErrorString("Unable to parse string as a time ",
                            time_format_sql));
        }
    }

    /**
     * Parses a String as an SQL timestamp.
     */
    public static Date toTimeStamp(String str) {
        synchronized (ts_format_sql) {
            for (DateFormat dateFormat : ts_format_sql) {
                try {
                    return dateFormat.parse(str);
                } catch (ParseException e) {
                }
            }
            throw new RuntimeException(
                    dateErrorString("Unable to parse string as a timestamp ",
                            ts_format_sql));
        }
    }


    /**
     * Casts a Java object to the SQL type specified by the given
     * DataTableColumnDef object.  This is used for the following engine
     * functions;
     * <ol>
     * <li> To prepare a value for insertion into the data store.  For example,
     *   the table column may be STRING but the value here is a BigNumber.
     * <li> To cast an object to a specific type in an SQL function such as
     *   CAST.
     * </ol>
     * Given any supported object, this will return the internal database
     * representation of the object as either NullObject, BigNumber, String,
     * Date, Boolean or ByteLongObject.
     *
     * @param ob the Object to cast to the given type
     * @param sql_type the enumerated sql type, eg. SQLTypes.LONGVARCHAR
     * @param sql_size the size of the type.  For example, CHAR(20)
     * @param sql_scale the scale of the numerical type.
     * @param sql_type_string 'sql_type' as a human understandable string,
     *   eg. "LONGVARCHAR"
     */
    public static Object castObjectToSQLType(Object ob,
                                             int sql_type, int sql_size, int sql_scale, String sql_type_string) {

//    if (ob == null) {
//      ob = NullObject.NULL_OBJ;
//    }

//    int sql_type = col_def.getSQLType();
//    int sql_size = col_def.getSize();
//    int sql_scale = col_def.getScale();
//    String sql_type_string = col_def.getSQLTypeString();

        // If the input object is a ByteLongObject and the output type is not a
        // binary SQL type then we need to attempt to deserialize the object.
        if (ob instanceof ByteLongObject) {
            if (sql_type != SQLTypes.JAVA_OBJECT &&
                    sql_type != SQLTypes.BLOB &&
                    sql_type != SQLTypes.BINARY &&
                    sql_type != SQLTypes.VARBINARY &&
                    sql_type != SQLTypes.LONGVARBINARY) {
                // Attempt to deserialize it
                try {
                    ob = ObjectTranslator.deserialize((ByteLongObject) ob);
                } catch (Throwable e) {
                    // Couldn't deserialize so it must be a standard blob which means
                    // we are in error.
                    throw new Error("Can't cast a BLOB to " + sql_type_string);
                }
            } else {
                // This is a ByteLongObject that is being cast to a binary type so
                // no further processing is necessary.
                return ob;
            }
        }

        // BlobRef can be BINARY, JAVA_OBJECT, VARBINARY or LONGVARBINARY
        if (ob instanceof BlobRef) {
            if (sql_type == SQLTypes.BINARY ||
                    sql_type == SQLTypes.BLOB ||
                    sql_type == SQLTypes.JAVA_OBJECT ||
                    sql_type == SQLTypes.VARBINARY ||
                    sql_type == SQLTypes.LONGVARBINARY) {
                return ob;
            }
        }

        // ClobRef can be VARCHAR, LONGVARCHAR, or CLOB
        if (ob instanceof ClobRef) {
            if (sql_type == SQLTypes.VARCHAR ||
                    sql_type == SQLTypes.LONGVARCHAR ||
                    sql_type == SQLTypes.CLOB) {
                return ob;
            }
        }

        // Cast from NULL
        if (ob == null) {
            switch (sql_type) {
                case (SQLTypes.BIT):
                    // fall through
                case (SQLTypes.TINYINT):
                    // fall through
                case (SQLTypes.SMALLINT):
                    // fall through
                case (SQLTypes.INTEGER):
                    // fall through
                case (SQLTypes.BIGINT):
                    // fall through
                case (SQLTypes.FLOAT):
                    // fall through
                case (SQLTypes.REAL):
                    // fall through
                case (SQLTypes.DOUBLE):
                    // fall through
                case (SQLTypes.NUMERIC):
                    // fall through
                case (SQLTypes.DECIMAL):
                    // fall through
                case (SQLTypes.CHAR):
                    // fall through
                case (SQLTypes.VARCHAR):
                    // fall through
                case (SQLTypes.LONGVARCHAR):
                    // fall through
                case (SQLTypes.CLOB):
                    // fall through
                case (SQLTypes.DATE):
                    // fall through
                case (SQLTypes.TIME):
                    // fall through
                case (SQLTypes.TIMESTAMP):
                    // fall through
                case (SQLTypes.NULL):
                    // fall through

                case (SQLTypes.BINARY):
                    // fall through
                case (SQLTypes.VARBINARY):
                    // fall through
                case (SQLTypes.LONGVARBINARY):
                    // fall through
                case (SQLTypes.BLOB):
                    // fall through


                case (SQLTypes.JAVA_OBJECT):
                    // fall through

                case (SQLTypes.BOOLEAN):
                    return null;
                default:
                    throw new Error("Can't cast NULL to " + sql_type_string);
            }
        }

        // Cast from a number
        if (ob instanceof Number) {
            Number n = (Number) ob;
            switch (sql_type) {
                case (SQLTypes.BIT):
                    return n.intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
                case (SQLTypes.TINYINT):
                    // fall through
                case (SQLTypes.SMALLINT):
                    // fall through
                case (SQLTypes.INTEGER):
//          return new BigDecimal(n.intValue());
                    return BigNumber.fromLong(n.intValue());
                case (SQLTypes.BIGINT):
//          return new BigDecimal(n.longValue());
                    return BigNumber.fromLong(n.longValue());
                case (SQLTypes.FLOAT):
                    return BigNumber.fromString(Double.toString(n.doubleValue()));
                case (SQLTypes.REAL):
                    return BigNumber.fromString(n.toString());
                case (SQLTypes.DOUBLE):
                    return BigNumber.fromString(Double.toString(n.doubleValue()));
                case (SQLTypes.NUMERIC):
                    // fall through
                case (SQLTypes.DECIMAL):
                    return BigNumber.fromString(n.toString());
                case (SQLTypes.CHAR):
                    return StringObject.fromString(paddedString(n.toString(), sql_size));
                case (SQLTypes.VARCHAR):
                    return StringObject.fromString(n.toString());
                case (SQLTypes.LONGVARCHAR):
                    return StringObject.fromString(n.toString());
                case (SQLTypes.DATE):
                    return toDate(n.longValue());
                case (SQLTypes.TIME):
                    return toDate(n.longValue());
                case (SQLTypes.TIMESTAMP):
                    return toDate(n.longValue());
                case (SQLTypes.BLOB):
                    // fall through
                case (SQLTypes.BINARY):
                    // fall through
                case (SQLTypes.VARBINARY):
                    // fall through
                case (SQLTypes.LONGVARBINARY):
                    return new ByteLongObject(n.toString().getBytes());
                case (SQLTypes.NULL):
                    return null;
                case (SQLTypes.JAVA_OBJECT):
                    return toJavaObject(ob);
                case (SQLTypes.BOOLEAN):
                    return n.intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
                default:
                    throw new Error("Can't cast number to " + sql_type_string);
            }
        }  // if (ob instanceof Number)

        // Cast from a string
        if (ob instanceof StringObject || ob instanceof String) {
            String str = ob.toString();
            switch (sql_type) {
                case (SQLTypes.BIT):
                    return str.equalsIgnoreCase("true") ? Boolean.TRUE : Boolean.FALSE;
                case (SQLTypes.TINYINT):
                    // fall through
                case (SQLTypes.SMALLINT):
                    // fall through
                case (SQLTypes.INTEGER):
//          return new BigDecimal(toBigDecimal(str).intValue());
                    return BigNumber.fromLong(toBigNumber(str).intValue());
                case (SQLTypes.BIGINT):
//          return new BigDecimal(toBigDecimal(str).longValue());
                    return BigNumber.fromLong(toBigNumber(str).longValue());
                case (SQLTypes.FLOAT):
                    return BigNumber.fromString(
                            Double.toString(toBigNumber(str).doubleValue()));
                case (SQLTypes.REAL):
                    return toBigNumber(str);
                case (SQLTypes.DOUBLE):
                    return BigNumber.fromString(
                            Double.toString(toBigNumber(str).doubleValue()));
                case (SQLTypes.NUMERIC):
                    // fall through
                case (SQLTypes.DECIMAL):
                    return toBigNumber(str);
                case (SQLTypes.CHAR):
                    return StringObject.fromString(paddedString(str, sql_size));
                case (SQLTypes.VARCHAR):
                    return StringObject.fromString(str);
                case (SQLTypes.LONGVARCHAR):
                    return StringObject.fromString(str);
                case (SQLTypes.DATE):
                    return toDate(str);
                case (SQLTypes.TIME):
                    return toTime(str);
                case (SQLTypes.TIMESTAMP):
                    return toTimeStamp(str);
                case (SQLTypes.BLOB):
                    // fall through
                case (SQLTypes.BINARY):
                    // fall through
                case (SQLTypes.VARBINARY):
                    // fall through
                case (SQLTypes.LONGVARBINARY):
                    return new ByteLongObject(str.getBytes());
                case (SQLTypes.NULL):
                    return null;
                case (SQLTypes.JAVA_OBJECT):
                    return toJavaObject(str);
                case (SQLTypes.BOOLEAN):
                    return str.equalsIgnoreCase("true") ? Boolean.TRUE : Boolean.FALSE;
                case (SQLTypes.CLOB):
                    return StringObject.fromString(str);
                default:
                    throw new Error("Can't cast string to " + sql_type_string);
            }
        }  // if (ob instanceof String)

        // Cast from a boolean
        if (ob instanceof Boolean) {
            Boolean b = (Boolean) ob;
            switch (sql_type) {
                case (SQLTypes.BIT):
                    return b;
                case (SQLTypes.TINYINT):
                    // fall through
                case (SQLTypes.SMALLINT):
                    // fall through
                case (SQLTypes.INTEGER):
                    // fall through
                case (SQLTypes.BIGINT):
                    // fall through
                case (SQLTypes.FLOAT):
                    // fall through
                case (SQLTypes.REAL):
                    // fall through
                case (SQLTypes.DOUBLE):
                    // fall through
                case (SQLTypes.NUMERIC):
                    // fall through
                case (SQLTypes.DECIMAL):
                    return b.equals(Boolean.TRUE) ? BD_ONE : BD_ZERO;
                case (SQLTypes.CHAR):
                    return StringObject.fromString(paddedString(b.toString(), sql_size));
                case (SQLTypes.VARCHAR):
                    return StringObject.fromString(b.toString());
                case (SQLTypes.LONGVARCHAR):
                    return StringObject.fromString(b.toString());
                case (SQLTypes.NULL):
                    return null;
                case (SQLTypes.JAVA_OBJECT):
                    return toJavaObject(ob);
                case (SQLTypes.BOOLEAN):
                    return b;
                default:
                    throw new Error("Can't cast boolean to " + sql_type_string);
            }
        }  // if (ob instanceof Boolean)

        // Cast from a date
        if (ob instanceof Date) {
            Date d = (Date) ob;
            switch (sql_type) {
                case (SQLTypes.TINYINT):
                    // fall through
                case (SQLTypes.SMALLINT):
                    // fall through
                case (SQLTypes.INTEGER):
                    // fall through
                case (SQLTypes.BIGINT):
                    // fall through
                case (SQLTypes.FLOAT):
                    // fall through
                case (SQLTypes.REAL):
                    // fall through
                case (SQLTypes.DOUBLE):
                    // fall through
                case (SQLTypes.NUMERIC):
                    // fall through
                case (SQLTypes.DECIMAL):
                    return BigNumber.fromLong(d.getTime());
                case (SQLTypes.CHAR):
                    return StringObject.fromString(paddedString(formatDateAsString(d), sql_size));
                case (SQLTypes.VARCHAR):
                    return StringObject.fromString(formatDateAsString(d));
                case (SQLTypes.LONGVARCHAR):
                    return StringObject.fromString(formatDateAsString(d));
                case (SQLTypes.DATE):
                    return d;
                case (SQLTypes.TIME):
                    return d;
                case (SQLTypes.TIMESTAMP):
                    return d;
                case (SQLTypes.NULL):
                    return null;
                case (SQLTypes.JAVA_OBJECT):
                    return toJavaObject(ob);
                default:
                    throw new Error("Can't cast date to " + sql_type_string);
            }
        }  // if (ob instanceof Date)

        // Some obscure types
        if (ob instanceof byte[]) {
            switch (sql_type) {
                case (SQLTypes.BLOB):
                    // fall through
                case (SQLTypes.BINARY):
                    // fall through
                case (SQLTypes.VARBINARY):
                    // fall through
                case (SQLTypes.LONGVARBINARY):
                    return new ByteLongObject((byte[]) ob);
                default:
                    throw new Error("Can't cast byte[] to " + sql_type_string);
            }
        }

        // Finally, the object can only be something that we can cast to a
        // JAVA_OBJECT.
        if (sql_type == SQLTypes.JAVA_OBJECT) {
            return toJavaObject(ob);
        }

        throw new RuntimeException("Can't cast object " + ob.getClass() + " to " +
                sql_type_string);

    }

}
