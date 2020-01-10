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
import java.io.ObjectOutputStream;
import java.util.Locale;

import com.pony.util.BigNumber;
import com.pony.database.global.ByteLongObject;
import com.pony.database.global.BlobRef;
import com.pony.database.global.ClobRef;
import com.pony.database.global.SQLTypes;
import com.pony.database.global.StringObject;

/**
 * A TObject is a strongly typed object in a database engine.  A TObject must
 * maintain type information (eg. STRING, NUMBER, etc) along with the
 * object value being represented itself.
 *
 * @author Tobias Downer
 */

public final class TObject implements java.io.Serializable {

    static final long serialVersionUID = -5129157457207765079L;

    /**
     * The type of this object.
     */
    private final TType type;

    /**
     * The Java representation of the object.
     */
    private Object ob;

    /**
     * Constructs the TObject as the given type.
     */
    public TObject(TType type, Object ob) {
        this.type = type;
        if (ob instanceof String) {
            this.ob = StringObject.fromString((String) ob);
        } else {
            this.ob = ob;
        }
    }

    /**
     * Returns the type of this object.
     */
    public TType getTType() {
        return type;
    }

    /**
     * Returns true if the object is null.  Note that we must still be able to
     * determine type information for an object that is NULL.
     */
    public boolean isNull() {
        return (getObject() == null);
    }

    /**
     * Returns a java.lang.Object that is the data behind this object.
     */
    public Object getObject() {
        return ob;
    }

    /**
     * Returns the approximate memory use of this object in bytes.  This is used
     * when the engine is caching objects and we need a general indication of how
     * much space it takes up in memory.
     */
    public int approximateMemoryUse() {
        return getTType().calculateApproximateMemoryUse(getObject());
    }

    /**
     * Returns true if the type of this object is logically comparable to the
     * type of the given object.  For example, VARCHAR and LONGVARCHAR are
     * comparable types.  DOUBLE and FLOAT are comparable types.  DOUBLE and
     * VARCHAR are not comparable types.
     */
    public boolean comparableTypes(TObject ob) {
        return getTType().comparableTypes(ob.getTType());
    }

    /**
     * Returns the BigNumber of this object if this object is a numeric type.  If
     * the object is not a numeric type or is NULL then a null object is
     * returned.  This method can not be used to cast from a type to a number.
     */
    public BigNumber toBigNumber() {
        if (getTType() instanceof TNumericType) {
            return (BigNumber) getObject();
        }
        return null;
    }

    /**
     * Returns the Boolean of this object if this object is a boolean type.  If
     * the object is not a boolean type or is NULL then a null object is
     * returned.  This method must not be used to cast from a type to a boolean.
     */
    public Boolean toBoolean() {
        if (getTType() instanceof TBooleanType) {
            return (Boolean) getObject();
        }
        return null;
    }

    /**
     * Returns the String of this object if this object is a string type.  If
     * the object is not a string type or is NULL then a null object is
     * returned.  This method must not be used to cast from a type to a string.
     */
    public String toStringValue() {
        if (getTType() instanceof TStringType) {
            return getObject().toString();
        }
        return null;
    }


    public static final TObject BOOLEAN_TRUE =
            new TObject(TType.BOOLEAN_TYPE, Boolean.TRUE);
    public static final TObject BOOLEAN_FALSE =
            new TObject(TType.BOOLEAN_TYPE, Boolean.FALSE);
    public static final TObject BOOLEAN_NULL =
            new TObject(TType.BOOLEAN_TYPE, null);

    public static final TObject NULL_OBJECT =
            new TObject(TType.NULL_TYPE, null);

    /**
     * Returns a TObject of boolean type that is either true or false.
     */
    public static TObject booleanVal(boolean b) {
        if (b) {
            return BOOLEAN_TRUE;
        }
        return BOOLEAN_FALSE;
    }

    /**
     * Returns a TObject of numeric type that represents the given int value.
     */
    public static TObject intVal(int val) {
        return bigNumberVal(BigNumber.fromLong(val));
    }

    /**
     * Returns a TObject of numeric type that represents the given long value.
     */
    public static TObject longVal(long val) {
        return bigNumberVal(BigNumber.fromLong(val));
    }

    /**
     * Returns a TObject of numeric type that represents the given double value.
     */
    public static TObject doubleVal(double val) {
        return bigNumberVal(BigNumber.fromDouble(val));
    }

    /**
     * Returns a TObject of numeric type that represents the given BigNumber
     * value.
     */
    public static TObject bigNumberVal(BigNumber val) {
        return new TObject(TType.NUMERIC_TYPE, val);
    }

    /**
     * Returns a TObject of VARCHAR type that represents the given StringObject
     * value.
     */
    public static TObject stringVal(StringObject str) {
        return new TObject(TType.STRING_TYPE, str);
    }

    /**
     * Returns a TObject of VARCHAR type that represents the given String value.
     */
    public static TObject stringVal(String str) {
        return new TObject(TType.STRING_TYPE, StringObject.fromString(str));
    }

    /**
     * Returns a TObject of DATE type that represents the given time value.
     */
    public static TObject dateVal(java.util.Date d) {
        return new TObject(TType.DATE_TYPE, d);
    }

    /**
     * Returns a TObject of NULL type that represents a null value.
     */
    public static TObject nullVal() {
        return NULL_OBJECT;
    }

    /**
     * Returns a TObject from the given Java value.
     */
    public static TObject objectVal(Object ob) {
        if (ob == null) {
            return nullVal();
        } else if (ob instanceof BigNumber) {
            return bigNumberVal((BigNumber) ob);
        } else if (ob instanceof StringObject) {
            return stringVal((StringObject) ob);
        } else if (ob instanceof Boolean) {
            return booleanVal((Boolean) ob);
        } else if (ob instanceof java.util.Date) {
            return dateVal((java.util.Date) ob);
        } else if (ob instanceof ByteLongObject) {
            return new TObject(TType.BINARY_TYPE, ob);
        } else if (ob instanceof byte[]) {
            return new TObject(TType.BINARY_TYPE, new ByteLongObject((byte[]) ob));
        } else if (ob instanceof BlobRef) {
            return new TObject(TType.BINARY_TYPE, ob);
        } else if (ob instanceof ClobRef) {
            return new TObject(TType.STRING_TYPE, ob);
        } else {
            throw new Error("Don't know how to convert object type " + ob.getClass());
        }
    }

    /**
     * Compares this object with the given object (which is of a logically
     * comparable type).  Returns 0 if the value of the objects are equal, < 0
     * if this object is smaller than the given object, and > 0 if this object
     * is greater than the given object.
     * <p>
     * This can not be used to compare null values so it assumes that checks
     * for null have already been made.
     */
    public int compareToNoNulls(TObject tob) {
        TType type = getTType();
        // Strings must be handled as a special case.
        if (type instanceof TStringType) {
            // We must determine the locale to compare against and use that.
            TStringType stype = (TStringType) type;
            // If there is no locale defined for this type we use the locale in the
            // given type.
            if (stype.getLocale() == null) {
                type = tob.getTType();
            }
        }
        return type.compareObs(getObject(), tob.getObject());
    }


    /**
     * Compares this object with the given object (which is of a logically
     * comparable type).  Returns 0 if the value of the objects are equal, < 0
     * if this object is smaller than the given object, and > 0 if this object
     * is greater than the given object.
     * <p>
     * This compares NULL values before non null values, and null values are
     * equal.
     */
    public int compareTo(TObject tob) {
        // If this is null
        if (isNull()) {
            // and value is null return 0 return less
            if (tob.isNull()) {
                return 0;
            } else {
                return -1;
            }
        } else {
            // If this is not null and value is null return +1
            if (tob.isNull()) {
                return 1;
            } else {
                // otherwise both are non null so compare normally.
                return compareToNoNulls(tob);
            }
        }
    }

    /**
     * Equality test.  This will throw an exception if it is used.  The reason
     * for this is because it's not clear what we would be testing the equality
     * of with this method.  Equality of the object + the type or equality of the
     * objects only?
     */
    public boolean equals(Object ob) {
        throw new Error("equals method should not be used.");
    }

    /**
     * Equality test.  Returns true if this object is equivalent to the given
     * TObject.  This means the types are the same, and the object itself is the
     * same.
     */
    public boolean valuesEqual(TObject ob) {
        if (this == ob) {
            return true;
        }
        if (getTType().comparableTypes(ob.getTType())) {
            return compareTo(ob) == 0;
        }
        return false;
    }


    // ---------- Object operators ----------

    /**
     * Bitwise OR operation of this object with the given object.  If either
     * numeric value has a scale of 1 or greater then it returns null.  If this
     * or the given object is not a numeric type then it returns null.  If either
     * this object or the given object is NULL, then the NULL object is returned.
     */
    public TObject operatorOr(TObject val) {
        BigNumber v1 = toBigNumber();
        BigNumber v2 = val.toBigNumber();
        TType result_type = TType.getWidestType(getTType(), val.getTType());

        if (v1 == null || v2 == null) {
            return new TObject(result_type, null);
        }

        return new TObject(result_type, v1.bitWiseOr(v2));
    }

    /**
     * Mathematical addition of this object to the given object.  If this or
     * the given object is not a numeric type then it returns null.
     * If either this object or the given object is NULL, then the NULL object
     * is returned.
     */
    public TObject operatorAdd(TObject val) {
        BigNumber v1 = toBigNumber();
        BigNumber v2 = val.toBigNumber();
        TType result_type = TType.getWidestType(getTType(), val.getTType());

        if (v1 == null || v2 == null) {
            return new TObject(result_type, null);
        }

        return new TObject(result_type, v1.add(v2));
    }

    /**
     * Mathematical subtraction of this object to the given object.  If this or
     * the given object is not a numeric type then it returns null.
     * If either this object or the given object is NULL, then the NULL object
     * is returned.
     */
    public TObject operatorSubtract(TObject val) {
        BigNumber v1 = toBigNumber();
        BigNumber v2 = val.toBigNumber();
        TType result_type = TType.getWidestType(getTType(), val.getTType());

        if (v1 == null || v2 == null) {
            return new TObject(result_type, null);
        }

        return new TObject(result_type, v1.subtract(v2));
    }

    /**
     * Mathematical multiply of this object to the given object.  If this or
     * the given object is not a numeric type then it returns null.
     * If either this object or the given object is NULL, then the NULL object
     * is returned.
     */
    public TObject operatorMultiply(TObject val) {
        BigNumber v1 = toBigNumber();
        BigNumber v2 = val.toBigNumber();
        TType result_type = TType.getWidestType(getTType(), val.getTType());

        if (v1 == null || v2 == null) {
            return new TObject(result_type, null);
        }

        return new TObject(result_type, v1.multiply(v2));
    }

    /**
     * Mathematical division of this object to the given object.  If this or
     * the given object is not a numeric type then it returns null.
     * If either this object or the given object is NULL, then the NULL object
     * is returned.
     */
    public TObject operatorDivide(TObject val) {
        BigNumber v1 = toBigNumber();
        BigNumber v2 = val.toBigNumber();
        TType result_type = TType.getWidestType(getTType(), val.getTType());

        if (v1 == null || v2 == null) {
            return new TObject(result_type, null);
        }

        return new TObject(result_type, v1.divide(v2));
    }

    /**
     * String concat of this object to the given object.  If this or the given
     * object is not a string type then it returns null.  If either this object
     * or the given object is NULL, then the NULL object is returned.
     * <p>
     * This operator always returns an object that is a VARCHAR string type of
     * unlimited size with locale inherited from either this or val depending
     * on whether the locale information is defined or not.
     */
    public TObject operatorConcat(TObject val) {

        // If this or val is null then return the null value
        if (isNull()) {
            return this;
        } else if (val.isNull()) {
            return val;
        }

        TType tt1 = getTType();
        TType tt2 = val.getTType();

        if (tt1 instanceof TStringType &&
                tt2 instanceof TStringType) {
            // Pick the first locale,
            TStringType st1 = (TStringType) tt1;
            TStringType st2 = (TStringType) tt2;

            Locale str_locale = null;
            int str_strength = 0;
            int str_decomposition = 0;

            if (st1.getLocale() != null) {
                str_locale = st1.getLocale();
                str_strength = st1.getStrength();
                str_decomposition = st1.getDecomposition();
            } else if (st2.getLocale() != null) {
                str_locale = st2.getLocale();
                str_strength = st2.getStrength();
                str_decomposition = st2.getDecomposition();
            }

            TStringType dest_type = st1;
            if (str_locale != null) {
                dest_type = new TStringType(SQLTypes.VARCHAR, -1,
                        str_locale, str_strength, str_decomposition);
            }

            return new TObject(dest_type,
                    StringObject.fromString(toStringValue() + val.toStringValue()));

        }

        // Return null if LHS or RHS are not strings
        return new TObject(tt1, null);
    }

    /**
     * Comparison of this object and the given object.  The compared objects
     * must be the same type otherwise it returns false.  This
     * is able to compare null values.
     */
    public TObject operatorIs(TObject val) {
        if (isNull() && val.isNull()) {
            return BOOLEAN_TRUE;
        }
        if (comparableTypes(val)) {
            return booleanVal(compareTo(val) == 0);
        }
        // Not comparable types so return false
        return BOOLEAN_FALSE;
    }

    /**
     * Comparison of this object and the given object.  The compared objects
     * must be the same type otherwise it returns null (doesn't know).  If either
     * this object or the given object is NULL then NULL is returned.
     */
    public TObject operatorEquals(TObject val) {
        // Check the types are comparable
        if (comparableTypes(val) && !isNull() && !val.isNull()) {
            return booleanVal(compareToNoNulls(val) == 0);
        }
        // Not comparable types so return null
        return BOOLEAN_NULL;
    }

    /**
     * Comparison of this object and the given object.  The compared objects
     * must be the same type otherwise it returns null (doesn't know).  If either
     * this object or the given object is NULL then NULL is returned.
     */
    public TObject operatorNotEquals(TObject val) {
        // Check the types are comparable
        if (comparableTypes(val) && !isNull() && !val.isNull()) {
            return booleanVal(compareToNoNulls(val) != 0);
        }
        // Not comparable types so return null
        return BOOLEAN_NULL;
    }

    /**
     * Comparison of this object and the given object.  The compared objects
     * must be the same type otherwise it returns null (doesn't know).  If either
     * this object or the given object is NULL then NULL is returned.
     */
    public TObject operatorGreater(TObject val) {
        // Check the types are comparable
        if (comparableTypes(val) && !isNull() && !val.isNull()) {
            return booleanVal(compareToNoNulls(val) > 0);
        }
        // Not comparable types so return null
        return BOOLEAN_NULL;
    }

    /**
     * Comparison of this object and the given object.  The compared objects
     * must be the same type otherwise it returns null (doesn't know).  If either
     * this object or the given object is NULL then NULL is returned.
     */
    public TObject operatorGreaterEquals(TObject val) {
        // Check the types are comparable
        if (comparableTypes(val) && !isNull() && !val.isNull()) {
            return booleanVal(compareToNoNulls(val) >= 0);
        }
        // Not comparable types so return null
        return BOOLEAN_NULL;
    }

    /**
     * Comparison of this object and the given object.  The compared objects
     * must be the same type otherwise it returns null (doesn't know).  If either
     * this object or the given object is NULL then NULL is returned.
     */
    public TObject operatorLess(TObject val) {
        // Check the types are comparable
        if (comparableTypes(val) && !isNull() && !val.isNull()) {
            return booleanVal(compareToNoNulls(val) < 0);
        }
        // Not comparable types so return null
        return BOOLEAN_NULL;
    }

    /**
     * Comparison of this object and the given object.  The compared objects
     * must be the same type otherwise it returns null (doesn't know).  If either
     * this object or the given object is NULL then NULL is returned.
     */
    public TObject operatorLessEquals(TObject val) {
        // Check the types are comparable
        if (comparableTypes(val) && !isNull() && !val.isNull()) {
            return booleanVal(compareToNoNulls(val) <= 0);
        }
        // Not comparable types so return null
        return BOOLEAN_NULL;
    }


    /**
     * Performs a logical NOT on this value.
     */
    public TObject operatorNot() {
        // If type is null
        if (isNull()) {
            return this;
        }
        Boolean b = toBoolean();
        if (b != null) {
            return booleanVal(!b);
        }
        return BOOLEAN_NULL;
    }


    // ---------- Casting methods -----------

    /**
     * Returns a TObject of the given type and with the given Java object.  If
     * the object is not of the right type then it is cast to the correct type.
     */
    public static TObject createAndCastFromObject(TType type, Object ob) {
        return new TObject(type, TType.castObjectToTType(ob, type));
    }

    /**
     * Casts this object to the given type and returns a new TObject.
     */
    public TObject castTo(TType cast_to_type) {
        Object ob = getObject();
        return createAndCastFromObject(cast_to_type, ob);
    }


    public String toString() {
        if (isNull()) {
            return "NULL";
        } else {
            return getObject().toString();
        }
    }


//  // ------ Default casting objects ----------
//  
//  /**
//   * Casts this object to a number.  If this object is NULL then the returned
//   * object is a numeric typed NULL.
//   */
//  public TObject castToNumber() {
//    if (getTType().isString()) {
//      try {
//        return new BigDecimal((String) ob);
//      }
//      catch (Throwable e) {
//        return BD_ZERO;
//      }
//    }
//    if (getTType().isBoolean()) {
//      if (((Boolean) ob).booleanValue() == true) {
//        return BD_ONE;
//      }
//      else {
//        return BD_ZERO;
//      }
//    }
//    if (getTType().isDate()) {
//      return new BigDecimal(((Date) ob).getTime());
//    }
//    return (BigDecimal) ob;
//  }
//
//
//  // ---------- Convenience statics ----------
//
//  private final static BigDecimal BD_ZERO = new BigDecimal(0);
//  private final static BigDecimal BD_ONE  = new BigDecimal(1);


    /**
     * Writes the state of this object to the object stream.  This method is
     * implemented because GCJ doesn't like it if you implement readObject
     * without writeObject.
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }

    /**
     * Serialization overwritten method.  We overwrite this method because of a
     * change with how strings are stored.  In 0.93 we stored strings in this
     * object as java.lang.String and in 0.94 we stored strings as
     * java.lang.StringObject.  This performs a conversion between the old and
     * new format.
     */
    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        // HACK: We convert old TObject that used String to represent a string object
        //  to StringObject
        if (ob instanceof String) {
            ob = StringObject.fromString((String) ob);
        }
    }

}

