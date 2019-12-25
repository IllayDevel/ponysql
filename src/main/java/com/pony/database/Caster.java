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

import com.pony.database.global.StringObject;
import com.pony.database.global.ByteLongObject;
import com.pony.database.global.ObjectTranslator;
import com.pony.database.global.SQLTypes;
import com.pony.util.BigNumber;

import java.lang.reflect.Constructor;

/**
 * Methods to choose and perform casts from database type to Java types.
 *
 * @author Jim McBeath
 */

public class Caster {

    /** The cost to cast to the closest Java primitive type. */
    public final static int PRIMITIVE_COST = 100;

    /** The cost to cast to the closes Java object type. */
    public final static int OBJECT_COST = 200;

    /** The maximum positive byte value as a BigNumber. */
    private final static BigNumber maxBigNumByte =
            BigNumber.fromInt(Byte.MAX_VALUE);

    /** The maximum positive byte value as a BigNumber. */
    private final static BigNumber minBigNumByte =
            BigNumber.fromInt(Byte.MIN_VALUE);

    /** The maximum positive short value as a BigNumber. */
    private final static BigNumber maxBigNumShort =
            BigNumber.fromInt(Short.MAX_VALUE);

    /** The maximum positive short value as a BigNumber. */
    private final static BigNumber minBigNumShort =
            BigNumber.fromInt(Short.MIN_VALUE);

    /** The maximum positive integer value as a BigNumber. */
    private final static BigNumber maxBigNumInt =
            BigNumber.fromInt(Integer.MAX_VALUE);

    /** The maximum positive integer value as a BigNumber. */
    private final static BigNumber minBigNumInt =
            BigNumber.fromInt(Integer.MIN_VALUE);

    /** The maximum positive long value as a BigNumber. */
    private final static BigNumber maxBigNumLong =
            BigNumber.fromLong(Long.MAX_VALUE);

    /** The maximum positive long value as a BigNumber. */
    private final static BigNumber minBigNumLong =
            BigNumber.fromLong(Long.MIN_VALUE);

    /** The maximum positive float value as a BigNumber. */
    private final static BigNumber maxBigNumFloat =
            BigNumber.fromDouble(Float.MAX_VALUE);

    /** The minimum positive float value as a BigNumber. */
    private static BigNumber minBigNumFloat =
            BigNumber.fromDouble(Float.MIN_VALUE);

    /** The maximum positive double value as a BigNumber. */
    private static BigNumber maxBigNumDouble =
            BigNumber.fromDouble(Double.MAX_VALUE);

    /**
     * Find any JAVA_OBJECTs in the args and deserialize them into
     * real Java objects.
     *
     * @param args The args to deserialize.  Any JAVA_OBJECT args are
     *        converted in-place to a new TObject with a value which is
     *        the deserialized object.
     */
    public static void deserializeJavaObjects(TObject[] args) {
        for (int i = 0; i < args.length; i++) {
            int sqlType = args[i].getTType().getSQLType();
            if (sqlType != SQLTypes.JAVA_OBJECT) {
                continue;    // not a JAVA_OBJECT
            }
            Object argVal = args[i].getObject();
            if (!(argVal instanceof ByteLongObject)) {
                continue;    // not ByteLongObject, we don't know how to deserialize
            }
            Object javaObj = ObjectTranslator.deserialize((ByteLongObject) argVal);
            args[i] = new TObject(args[i].getTType(), javaObj);
        }
    }

    /**
     * Search for the best constructor that we can use with the given
     * argument types.
     *
     * @param constructs The set of constructors from which to select.
     * @param argSqlTypes The SQL types of the database arguments to be passed
     *        to the constructor.
     * @return The constructor with the lowest cost, or null if there
     *         are no constructors that match the args.
     */
    public static Constructor findBestConstructor(
            Constructor[] constructs, TObject[] args) {
        int bestCost = 0;        // not used if bestConstructor is null
        Constructor bestConstructor = null;
        int[] argSqlTypes = getSqlTypes(args);
        for (int i = 0; i < constructs.length; ++i) {
            Class[] targets = constructs[i].getParameterTypes();
            int cost = getCastingCost(args, argSqlTypes, targets);
            if (cost < 0) {
                continue;        // not a usable constructor
            }
            if (bestConstructor == null || cost < bestCost) {
                bestCost = cost;    // found a better one, remember it
                bestConstructor = constructs[i];
            }
        }
        return bestConstructor;    // null if we didn't find any
    }

    /**
     * Get the SQL types for the given database arguments.
     *
     * @param args The database args.
     * @return The SQL types of the args.
     */
    public static int[] getSqlTypes(TObject[] args) {
        int[] sqlTypes = new int[args.length];
        for (int i = 0; i < args.length; i++) {
            sqlTypes[i] = getSqlType(args[i]);
        }
        return sqlTypes;
    }

    /**
     * Get the SQL type for a database argument.
     * If the actual value does not fit into the declared type, the returned
     * type is widened as required for the value to fit.
     *
     * @param arg The database argument.
     * @return The SQL type of the arg.
     */
    public static int getSqlType(TObject arg) {
        int sqlType = arg.getTType().getSQLType();
        Object argVal = arg.getObject();
        if (!(argVal instanceof BigNumber)) {
            return sqlType;    // We have special checks only for numeric values
        }
        BigNumber b = (BigNumber) argVal;
        BigNumber bAbs;
        switch (sqlType) {
            case SQLTypes.NUMERIC:
            case SQLTypes.DECIMAL:
                // If the type is NUMERIC or DECIMAL, then look at the data value
                // to see if it can be narrowed to int, long or double.
                if (b.canBeRepresentedAsInt()) {
                    sqlType = SQLTypes.INTEGER;
                } else if (b.canBeRepresentedAsLong()) {
                    sqlType = SQLTypes.BIGINT;
                } else {
                    bAbs = b.abs();
                    if (b.getScale() == 0) {
                        if (bAbs.compareTo(maxBigNumInt) <= 0) {
                            sqlType = SQLTypes.INTEGER;
                        } else if (bAbs.compareTo(maxBigNumLong) <= 0) {
                            sqlType = SQLTypes.BIGINT;
                        }
                    } else if (bAbs.compareTo(maxBigNumDouble) <= 0) {
                        sqlType = SQLTypes.DOUBLE;
                    }
                }
                // If we can't translate NUMERIC or DECIMAL to int, long or double,
                // then leave it as is.
                break;
            case SQLTypes.BIT:
                if (b.canBeRepresentedAsInt()) {
                    int n = b.intValue();
                    if (n == 0 || n == 1) {
                        return sqlType;    // Allowable BIT value
                    }
                }
                // The value does not fit in a BIT, move up to a TINYINT
                sqlType = SQLTypes.TINYINT;
                // FALL THROUGH
            case SQLTypes.TINYINT:
                if (b.compareTo(maxBigNumByte) <= 0 &&
                        b.compareTo(minBigNumByte) >= 0) {
                    return sqlType;    // Fits in a TINYINT
                }
                // The value does not fit in a TINYINT, move up to a SMALLINT
                sqlType = SQLTypes.SMALLINT;
                // FALL THROUGH
            case SQLTypes.SMALLINT:
                if (b.compareTo(maxBigNumShort) <= 0 &&
                        b.compareTo(minBigNumShort) >= 0) {
                    return sqlType;    // Fits in a SMALLINT
                }
                // The value does not fit in a SMALLINT, move up to a INTEGER
                sqlType = SQLTypes.INTEGER;
                // FALL THROUGH
            case SQLTypes.INTEGER:
                if (b.compareTo(maxBigNumInt) <= 0 &&
                        b.compareTo(minBigNumInt) >= 0) {
                    return sqlType;    // Fits in a INTEGER
                }
                // The value does not fit in a INTEGER, move up to a BIGINT
                sqlType = SQLTypes.BIGINT;
                // That's as far as we go
                break;
            case SQLTypes.REAL:
                bAbs = b.abs();
                if (bAbs.compareTo(maxBigNumFloat) <= 0 &&
                        (bAbs.compareTo(minBigNumFloat) >= 0 ||
                                b.doubleValue() == 0.0)) {
                    return sqlType;    // Fits in a REAL
                }
                // The value does not fit in a REAL, move up to a DOUBLE
                sqlType = SQLTypes.DOUBLE;
                break;
            default:
                break;
        }
        return sqlType;
    }

    /**
     * Get a string giving the database types of all of the arguments.
     * Useful for error messages.
     *
     * @param args The arguments.
     * @return A string with the types of all of the arguments,
     *         using comma as a separator.
     */
    public static String getArgTypesString(TObject[] args) {
        StringBuffer sb = new StringBuffer();
        for (int n = 0; n < args.length; n++) {
            if (n > 0) {
                sb.append(",");
            }
            if (args[n] == null) {
                sb.append("null");
            } else {
                int sqlType = getSqlType(args[n]);
                String typeName;
                if (sqlType == SQLTypes.JAVA_OBJECT) {
                    Object argObj = args[n].getObject();
                    if (argObj == null) {
                        typeName = "null";
                    } else {
                        typeName = argObj.getClass().getName();
                    }
                } else {
                    typeName = DataTableColumnDef.sqlTypeToString(sqlType);
                }
                sb.append(typeName);
            }
        }
        return sb.toString();
    }

    /**
     * Get the cost for casting the given arg types
     * to the desired target classes.
     *
     * @param args The database arguments from which we are casting.
     * @param argSqlTypes The SQL types of the args.
     * @param targets The java classes to which we are casting.
     * @return The cost of doing the cast for all arguments,
     *         or -1 if the args can not be cast to the targets.
     */
    static int getCastingCost(TObject[] args, int[] argSqlTypes,
                              Class[] targets) {
        if (targets.length != argSqlTypes.length) {
            return -1;        // wrong number of args
        }

        // Sum up the cost of converting each arg
        int totalCost = 0;
        for (int n = 0; n < argSqlTypes.length; ++n) {
            int argCost = getCastingCost(args[n], argSqlTypes[n], targets[n]);
            if (argCost < 0) {
                return -1;        //can't cast this arg type
            }
            int positionalCost = argCost * n / 10000;
            //Add a little bit to disambiguate constructors based on
            //argument position.  This gives preference to earlier
            //argument in cases where the cost of two sets of
            //targets for the same set of args would otherwise
            //be the same.
            totalCost += argCost + positionalCost;
        }
        return totalCost;
    }

    // These arrays are used in the getCastingCost method below.
    private static String[] bitPrims = {"boolean"};
    private static Class[] bitClasses = {Boolean.class};

    private static String[] tinyPrims = {"byte", "short", "int", "long"};
    private static Class[] tinyClasses = {Byte.class, Short.class,
            Integer.class, Long.class, Number.class};

    private static String[] smallPrims = {"short", "int", "long"};
    private static Class[] smallClasses = {Short.class, Integer.class,
            Long.class, Number.class};

    private static String[] intPrims = {"int", "long"};
    private static Class[] intClasses = {Integer.class, Long.class,
            Number.class};

    private static String[] bigPrims = {"long"};
    private static Class[] bigClasses = {Long.class, Number.class};

    private static String[] floatPrims = {"float", "double"};
    private static Class[] floatClasses = {Float.class, Double.class,
            Number.class};

    private static String[] doublePrims = {"double"};
    private static Class[] doubleClasses = {Double.class, Number.class};

    private static String[] stringPrims = {};
    private static Class[] stringClasses = {String.class};

    private static String[] datePrims = {};
    private static Class[] dateClasses = {java.sql.Date.class,
            java.util.Date.class};

    private static String[] timePrims = {};
    private static Class[] timeClasses = {java.sql.Time.class,
            java.util.Date.class};

    private static String[] timestampPrims = {};
    private static Class[] timestampClasses = {java.sql.Timestamp.class,
            java.util.Date.class};

    /**
     * Get the cost to cast an SQL type to the desired target class.
     * The cost is 0 to cast to TObject,
     * 100 to cast to the closest primitive,
     * or 200 to cast to the closest Object,
     * plus 1 for each widening away from the closest.
     *
     * @param arg The argument to cast.
     * @param argSqlType The SQL type of the arg.
     * @param target The target to which to cast.
     * @return The cost to do the cast, or -1 if the cast can not be done.
     */
    static int getCastingCost(TObject arg, int argSqlType, Class target) {

        //If the user has a method that takes a TObject, assume he can handle
        //anything.
        if (target == TObject.class) {
            return 0;
        }

        switch (argSqlType) {

            case SQLTypes.BIT:
                return getCastingCost(arg, bitPrims, bitClasses, target);

            case SQLTypes.TINYINT:
                return getCastingCost(arg, tinyPrims, tinyClasses, target);

            case SQLTypes.SMALLINT:
                return getCastingCost(arg, smallPrims, smallClasses, target);

            case SQLTypes.INTEGER:
                return getCastingCost(arg, intPrims, intClasses, target);

            case SQLTypes.BIGINT:
                return getCastingCost(arg, bigPrims, bigClasses, target);

            case SQLTypes.REAL:
                return getCastingCost(arg, floatPrims, floatClasses, target);

            case SQLTypes.FLOAT:
            case SQLTypes.DOUBLE:
                return getCastingCost(arg, doublePrims, doubleClasses, target);

            // We only get a NUMERIC or DECIMAL type here if we were not able to
            // convert it to int, long or double, so we can't handle it.  For now we
            // require that these types be handled by a method that takes a TObject.
            // That gets checked at the top of this method, so if we get to here
            // the target is not a TOBject, so we don't know how to handle it.
            case SQLTypes.NUMERIC:
            case SQLTypes.DECIMAL:
                return -1;

            case SQLTypes.CHAR:
            case SQLTypes.VARCHAR:
            case SQLTypes.LONGVARCHAR:
                return getCastingCost(arg, stringPrims, stringClasses, target);

            case SQLTypes.DATE:
                return getCastingCost(arg, datePrims, dateClasses, target);

            case SQLTypes.TIME:
                return getCastingCost(arg, timePrims, timeClasses, target);

            case SQLTypes.TIMESTAMP:
                return getCastingCost(arg, timestampPrims, timestampClasses, target);

            case SQLTypes.BINARY:
            case SQLTypes.VARBINARY:
            case SQLTypes.LONGVARBINARY:
                return -1;    // Can't handle these, user must use TObject

            // We can cast a JAVA_OBJECT only if the value is a subtype of the
            // target class.
            case SQLTypes.JAVA_OBJECT:
                Object argVal = arg.getObject();
                if (argVal == null || target.isAssignableFrom(argVal.getClass())) {
                    return OBJECT_COST;
                }
                return -1;

            // If the declared data type is NULL, then we have no type info to
            // determine how to cast it.
            case SQLTypes.NULL:
                return -1;

            default:
                return -1;    // Don't know how to cast other types
        }
    }

    /**
     * Get the cost to cast to the specified target from the set of
     * allowable primitives and object classes.
     *
     * @param arg The value being cast.
     * @param prims The set of allowable Java primitive types to which we can
     *              cast, ordered with the preferred types first.
     *              If the value of the arg is null, it can not be cast to a
     *              primitive type.
     * @param objects The set of allowable Java Object types to which we can
     *                cast, ordered with the preferred types first.
     * @param target The target class to which we are casting.
     * @return The cost of the cast, or -1 if the cast is not allowed.
     */
    static int getCastingCost(TObject arg, String[] prims, Class[] objects,
                              Class target) {
        if (target.isPrimitive()) {
            Object argVal = arg.getObject();    // get the vaue of the arg
            if (argVal == null) {
                return -1;    // can't cast null to a primitive
            }
            String targetName = target.getName();
            // Look for the closest allowable primitive
            for (int i = 0; i < prims.length; i++) {
                if (targetName.equals(prims[i]))
                    return PRIMITIVE_COST + i;
                // Cost of casting to a primitive plus the widening cost (i)
            }
        } else {
            // Look for the closest allowable object class
            for (int i = 0; i < objects.length; i++) {
                if (objects[i].isAssignableFrom(target))
                    return OBJECT_COST + i;
                // Cost of casting to an object class plus the widening cost (i)
            }
        }
        return -1;    // can't cast it
    }

    /**
     * Cast the given arguments to the specified constructors parameter types.
     * The caller must already have checked to make sure the argument count
     * and types match the constructor.
     *
     * @param args The database arguments from which to cast.
     * @param constructor The constructor to which to cast.
     * @return The cast arguments.
     */
    public static Object[] castArgsToConstructor(
            TObject[] args, Constructor constructor) {
        Class[] targets = constructor.getParameterTypes();
        return castArgs(args, targets);
    }

    /**
     * Cast the given arguments to the specified classes.
     * The caller must already have checked to make sure the argument count
     * and types match the constructor.
     *
     * @param args The database arguments from which to cast.
     * @param targets The java classes to which to cast.
     * @return The cast arguments.
     */
    static Object[] castArgs(TObject[] args, Class[] targets) {
        if (targets.length != args.length) {
            // we shouldn't get this error
            throw new RuntimeException("array length mismatch: arg=" + args.length +
                    ", targets=" + targets.length);
        }
        Object[] castedArgs = new Object[args.length];
        for (int n = 0; n < args.length; ++n) {
            castedArgs[n] = castArg(args[n], targets[n]);
        }
        return castedArgs;
    }

    /**
     * Cast the object to the specified target.
     *
     * @param arg The database argumument from which to cast.
     * @param target The java class to which to cast.
     * @return The cast object.
     */
    static Object castArg(TObject arg, Class target) {
        // By the time we get here, we have already run through the cost function
        // and eliminated the casts that don't work, including not allowing a null
        // value to be cast to a primitive type.

        if (target == TObject.class) {
            return arg;
        }

        Object argVal = arg.getObject();
        if (argVal == null) {
            // If the arg is null, then we must be casting to an Object type,
            // so just return null.
            return null;
        }

        //boolean isPrimitive = target.isPrimitive();
        String targetName = target.getName();

        if (argVal instanceof Boolean) {
            //BIT
            if (targetName.equals("boolean") ||
                    Boolean.class.isAssignableFrom(target)) {
                return argVal;
            }
        } else if (argVal instanceof Number) {
            //TINYINT, SMALLINT, INTEGER, BIGINT,
            //REAL, FLOAT, DOUBLE, NUMERIC, DECIMAL
            Number num = (Number) argVal;
            if (targetName.equals("byte") || Byte.class.isAssignableFrom(target)) {
                return new Byte(num.byteValue());
            }
            if (targetName.equals("short") || Short.class.isAssignableFrom(target)) {
                return new Short(num.shortValue());
            }
            if (targetName.equals("int") || Integer.class.isAssignableFrom(target)) {
                return new Integer(num.intValue());
            }
            if (targetName.equals("long") || Long.class.isAssignableFrom(target)) {
                return new Long(num.longValue());
            }
            if (targetName.equals("float") || Float.class.isAssignableFrom(target)) {
                return new Float(num.floatValue());
            }
            if (targetName.equals("double") ||
                    Double.class.isAssignableFrom(target)) {
                return new Float(num.doubleValue());
            }
        } else if (argVal instanceof java.util.Date) {
            //DATE, TIME, TIMESTAMP
            java.util.Date date = (java.util.Date) argVal;
            if (java.sql.Date.class.isAssignableFrom(target)) {
                return new java.sql.Date(date.getTime());
            }
            if (java.sql.Time.class.isAssignableFrom(target)) {
                return new java.sql.Time(date.getTime());
            }
            if (java.sql.Timestamp.class.isAssignableFrom(target)) {
                return new java.sql.Timestamp(date.getTime());
            }
            if (java.util.Date.class.isAssignableFrom(target)) {
                return date;
            }
        } else if (argVal instanceof String ||
                argVal instanceof StringObject) {
            //CHAR, VARCHAR, LONGVARCHAR
            String s = argVal.toString();
            if (String.class.isAssignableFrom(target)) {
                return s;
            }
        } else if (getSqlType(arg) == SQLTypes.JAVA_OBJECT) {
            // JAVA_OBJECT
            if (target.isAssignableFrom(argVal.getClass())) {
                return argVal;
            }
        } else {
            // BINARY, VARBINARY, LONGVARBINARY
            // NULL
            // We don't know how to handle any of these except as TObject
        }

        // Can't cast - we should not get here, since we checked for the
        // legality of the cast when calculating the cost.  However, the
        // code to do the cost is not the same as the code to do the casting,
        // so we may have messed up in one or the other.

        throw new RuntimeException("Programming error: Can't cast from " +
                argVal.getClass().getName() + " to " + target.getName());
    }

}
