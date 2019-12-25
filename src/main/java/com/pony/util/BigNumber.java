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

package com.pony.util;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Extends BigDecimal to allow a number to be positive infinity, negative
 * infinity and not-a-number.  This provides compatibility with float and
 * double types.
 *
 * @author Tobias Downer
 */

public final class BigNumber extends Number {

    static final long serialVersionUID = -8681578742639638105L;

    /**
     * State enumerations.
     */
    private final static byte NEG_INF_STATE = 1;
    private final static byte POS_INF_STATE = 2;
    private final static byte NaN_STATE = 3;

    /**
     * The state of the number, either 0 for number is the BigDecimal, 1 for
     * negative infinity, 2 for positive infinity and 3 for NaN.
     */
    private byte number_state;

    /**
     * The BigDecimal representation.
     */
    private BigDecimal big_decimal;

    /**
     * A 'long' representation of this number.
     */
    private long long_representation;

    /**
     * If this can be represented as an int or long, this contains the number
     * of bytes needed to represent the number.
     */
    private byte byte_count = 120;

    /**
     * Constructs the number.
     */
    private BigNumber(byte number_state, BigDecimal big_decimal) {
        this.number_state = number_state;
        if (number_state == 0) {
            setBigDecimal(big_decimal);
        }
    }

    private BigNumber(byte[] buf, int scale, byte state) {
        this.number_state = state;
        if (number_state == 0) {
            BigInteger bigint = new BigInteger(buf);
            setBigDecimal(new BigDecimal(bigint, scale));
        }
    }

    // Only call this from a constructor!
    private void setBigDecimal(BigDecimal big_decimal) {
        this.big_decimal = big_decimal;
        if (big_decimal.scale() == 0) {
            BigInteger bint = big_decimal.toBigInteger();
            int bit_count = big_decimal.toBigInteger().bitLength();
            if (bit_count < 30) {
                this.long_representation = bint.longValue();
                this.byte_count = 4;
            } else if (bit_count < 60) {
                this.long_representation = bint.longValue();
                this.byte_count = 8;
            }
        }
    }

    /**
     * Returns true if this BigNumber can be represented by a 64-bit long (has
     * no scale).
     */
    public boolean canBeRepresentedAsLong() {
        return byte_count <= 8;
    }

    /**
     * Returns true if this BigNumber can be represented by a 32-bit int (has
     * no scale).
     */
    public boolean canBeRepresentedAsInt() {
        return byte_count <= 4;
    }

    /**
     * Returns the scale of this number, or -1 if the number has no scale (if
     * it -inf, +inf or NaN).
     */
    public int getScale() {
        if (number_state == 0) {
            return big_decimal.scale();
        } else {
            return -1;
        }
    }

    /**
     * Returns the state of this number.  Returns either 1 which indicates
     * negative infinity, 2 which indicates positive infinity, or 3 which
     * indicates NaN.
     */
    public byte getState() {
        return number_state;
    }

    /**
     * Returns the inverse of the state.
     */
    private byte getInverseState() {
        if (number_state == NEG_INF_STATE) {
            return POS_INF_STATE;
        } else if (number_state == POS_INF_STATE) {
            return NEG_INF_STATE;
        } else {
            return number_state;
        }
    }

    /**
     * Returns this number as a byte array (unscaled).
     */
    public byte[] toByteArray() {
        if (number_state == 0) {
            return big_decimal.movePointRight(
                    big_decimal.scale()).toBigInteger().toByteArray();
// [ NOTE: The following code is 1.2+ only but BigNumber should be compatible
//         with 1.1 so we use the above call ]
//    return big_decimal.unscaledValue().toByteArray();
        } else {
            return new byte[0];
        }
    }

    /**
     * Returns this big number as a string.
     */
    public String toString() {
        switch (number_state) {
            case (0):
                return big_decimal.toString();
            case (NEG_INF_STATE):
                return "-Infinity";
            case (POS_INF_STATE):
                return "Infinity";
            case (NaN_STATE):
                return "NaN";
            default:
                throw new Error("Unknown number state");
        }
    }

    /**
     * Returns this big number as a double.
     */
    public double doubleValue() {
        switch (number_state) {
            case (0):
                return big_decimal.doubleValue();
            case (NEG_INF_STATE):
                return Double.NEGATIVE_INFINITY;
            case (POS_INF_STATE):
                return Double.POSITIVE_INFINITY;
            case (NaN_STATE):
                return Double.NaN;
            default:
                throw new Error("Unknown number state");
        }
    }

    /**
     * Returns this big number as a float.
     */
    public float floatValue() {
        switch (number_state) {
            case (0):
                return big_decimal.floatValue();
            case (NEG_INF_STATE):
                return Float.NEGATIVE_INFINITY;
            case (POS_INF_STATE):
                return Float.POSITIVE_INFINITY;
            case (NaN_STATE):
                return Float.NaN;
            default:
                throw new Error("Unknown number state");
        }
    }

    /**
     * Returns this big number as a long.
     */
    public long longValue() {
        if (canBeRepresentedAsLong()) {
            return long_representation;
        }
        switch (number_state) {
            case (0):
                return big_decimal.longValue();
            default:
                return (long) doubleValue();
        }
    }

    /**
     * Returns this big number as an int.
     */
    public int intValue() {
        if (canBeRepresentedAsLong()) {
            return (int) long_representation;
        }
        switch (number_state) {
            case (0):
                return big_decimal.intValue();
            default:
                return (int) doubleValue();
        }
    }

    /**
     * Returns this big number as a short.
     */
    public short shortValue() {
        return (short) intValue();
    }

    /**
     * Returns this big number as a byte.
     */
    public byte byteValue() {
        return (byte) intValue();
    }


    /**
     * Returns the big number as a BigDecimal object.  Note that this throws
     * an arith error if this number represents NaN, +Inf or -Inf.
     */
    public BigDecimal asBigDecimal() {
        if (number_state == 0) {
            return big_decimal;
        } else {
            throw new ArithmeticException(
                    "NaN, +Infinity or -Infinity can't be translated to a BigDecimal");
        }
    }

    /**
     * Compares this BigNumber with the given BigNumber.  Returns 0 if the values
     * are equal, >0 if this is greater than the given value, and &lt; 0 if this
     * is less than the given value.
     */
    public int compareTo(BigNumber number) {

        if (this == number) {
            return 0;
        }

        // If this is a non-infinity number
        if (number_state == 0) {

            // If both values can be represented by a long value
            if (canBeRepresentedAsLong() && number.canBeRepresentedAsLong()) {
                // Perform a long comparison check,
                if (long_representation > number.long_representation) {
                    return 1;
                } else if (long_representation < number.long_representation) {
                    return -1;
                } else {
                    return 0;
                }

            }

            // And the compared number is non-infinity then use the BigDecimal
            // compareTo method.
            if (number.number_state == 0) {
                return big_decimal.compareTo(number.big_decimal);
            } else {
                // Comparing a regular number with a NaN number.
                // If positive infinity or if NaN
                if (number.number_state == POS_INF_STATE ||
                        number.number_state == NaN_STATE) {
                    return -1;
                }
                // If negative infinity
                else if (number.number_state == NEG_INF_STATE) {
                    return 1;
                } else {
                    throw new Error("Unknown number state.");
                }
            }
        } else {
            // This number is a NaN number.
            // Are we comparing with a regular number?
            if (number.number_state == 0) {
                // Yes, negative infinity
                if (number_state == NEG_INF_STATE) {
                    return -1;
                }
                // positive infinity or NaN
                else if (number_state == POS_INF_STATE ||
                        number_state == NaN_STATE) {
                    return 1;
                } else {
                    throw new Error("Unknown number state.");
                }
            } else {
                // Comparing NaN number with a NaN number.
                // This compares -Inf less than Inf and NaN and NaN greater than
                // Inf and -Inf.  -Inf < Inf < NaN
                return (int) (number_state - number.number_state);
            }
        }
    }

    /**
     * The equals comparison uses the BigDecimal 'equals' method to compare
     * values.  This means that '0' is NOT equal to '0.0' and '10.0' is NOT equal
     * to '10.00'.  Care should be taken when using this method.
     */
    public boolean equals(Object ob) {
        BigNumber bnum = (BigNumber) ob;
        if (number_state != 0) {
            return (number_state == bnum.number_state);
        } else {
            return big_decimal.equals(bnum.big_decimal);
        }
    }


    /**
     * Statics.
     */
    private final static BigDecimal BD_ZERO = new BigDecimal(0);


    // ---- Mathematical functions ----

    public BigNumber bitWiseOr(BigNumber number) {
        if (number_state == 0 && getScale() == 0 &&
                number.number_state == 0 && number.getScale() == 0) {
            BigInteger bi1 = big_decimal.toBigInteger();
            BigInteger bi2 = number.big_decimal.toBigInteger();
            return new BigNumber((byte) 0, new BigDecimal(bi1.or(bi2)));
        } else {
            return null;
        }
    }

    public BigNumber add(BigNumber number) {
        if (number_state == 0) {
            if (number.number_state == 0) {
                return new BigNumber((byte) 0, big_decimal.add(number.big_decimal));
            } else {
                return new BigNumber(number.number_state, null);
            }
        } else {
            return new BigNumber(number_state, null);
        }
    }

    public BigNumber subtract(BigNumber number) {
        if (number_state == 0) {
            if (number.number_state == 0) {
                return new BigNumber((byte) 0, big_decimal.subtract(number.big_decimal));
            } else {
                return new BigNumber(number.getInverseState(), null);
            }
        } else {
            return new BigNumber(number_state, null);
        }
    }

    public BigNumber multiply(BigNumber number) {
        if (number_state == 0) {
            if (number.number_state == 0) {
                return new BigNumber((byte) 0, big_decimal.multiply(number.big_decimal));
            } else {
                return new BigNumber(number.number_state, null);
            }
        } else {
            return new BigNumber(number_state, null);
        }
    }

    public BigNumber divide(BigNumber number) {
        if (number_state == 0) {
            if (number.number_state == 0) {
                BigDecimal div_by = number.big_decimal;
                if (div_by.compareTo(BD_ZERO) != 0) {
                    return new BigNumber((byte) 0,
                            big_decimal.divide(div_by, 10, BigDecimal.ROUND_HALF_UP));
                }
            }
        }
        // Return NaN if we can't divide
        return new BigNumber((byte) 3, null);
    }

    public BigNumber abs() {
        if (number_state == 0) {
            return new BigNumber((byte) 0, big_decimal.abs());
        } else if (number_state == NEG_INF_STATE) {
            return new BigNumber(POS_INF_STATE, null);
        } else {
            return new BigNumber(number_state, null);
        }
    }

    public int signum() {
        if (number_state == 0) {
            return big_decimal.signum();
        } else if (number_state == NEG_INF_STATE) {
            return -1;
        } else {
            return 1;
        }
    }

    public BigNumber setScale(int d, int round_enum) {
        if (number_state == 0) {
            return new BigNumber((byte) 0, big_decimal.setScale(d, round_enum));
        }
        // Can't round -inf, +inf and NaN
        return this;
    }

    public BigNumber sqrt() {
        double d = doubleValue();
        d = Math.sqrt(d);
        return fromDouble(d);
    }


    // ---------- Casting from java types ----------

    /**
     * Creates a BigNumber from a double.
     */
    public static BigNumber fromDouble(double value) {
        if (value == Double.NEGATIVE_INFINITY) {
            return NEGATIVE_INFINITY;
        } else if (value == Double.POSITIVE_INFINITY) {
            return POSITIVE_INFINITY;
        } else if (value != value) {
            return NaN;
        }
        return new BigNumber((byte) 0, new BigDecimal(Double.toString(value)));
    }

    /**
     * Creates a BigNumber from a float.
     */
    public static BigNumber fromFloat(float value) {
        if (value == Float.NEGATIVE_INFINITY) {
            return NEGATIVE_INFINITY;
        } else if (value == Float.POSITIVE_INFINITY) {
            return POSITIVE_INFINITY;
        } else if (value != value) {
            return NaN;
        }
        return new BigNumber((byte) 0, new BigDecimal(Float.toString(value)));
    }

    /**
     * Creates a BigNumber from a long.
     */
    public static BigNumber fromLong(long value) {
        return new BigNumber((byte) 0, BigDecimal.valueOf(value));
    }

    /**
     * Creates a BigNumber from an int.
     */
    public static BigNumber fromInt(int value) {
        return new BigNumber((byte) 0, BigDecimal.valueOf(value));
    }

    /**
     * Creates a BigNumber from a string.
     */
    public static BigNumber fromString(String str) {
        if (str.equals("Infinity")) {
            return POSITIVE_INFINITY;
        } else if (str.equals("-Infinity")) {
            return NEGATIVE_INFINITY;
        } else if (str.equals("NaN")) {
            return NaN;
        } else {
            return new BigNumber((byte) 0, new BigDecimal(str));
        }
    }

    /**
     * Creates a BigNumber from a BigDecimal.
     */
    public static BigNumber fromBigDecimal(BigDecimal val) {
        return new BigNumber((byte) 0, val);
    }

    /**
     * Creates a BigNumber from the given data.
     */
    public static BigNumber fromData(byte[] buf, int scale, byte state) {
        if (state == 0) {
            // This inlines common numbers to save a bit of memory.
            if (scale == 0 && buf.length == 1) {
                if (buf[0] == 0) {
                    return BIG_NUMBER_ZERO;
                } else if (buf[0] == 1) {
                    return BIG_NUMBER_ONE;
                }
            }
            return new BigNumber(buf, scale, state);
        } else if (state == NEG_INF_STATE) {
            return NEGATIVE_INFINITY;
        } else if (state == POS_INF_STATE) {
            return POSITIVE_INFINITY;
        } else if (state == NaN_STATE) {
            return NaN;
        } else {
            throw new Error("Unknown number state.");
        }
    }


    /**
     * Statics for negative infinity, positive infinity and NaN.
     */
    public static final BigNumber NEGATIVE_INFINITY =
            new BigNumber(NEG_INF_STATE, null);
    public static final BigNumber POSITIVE_INFINITY =
            new BigNumber(POS_INF_STATE, null);
    public static final BigNumber NaN = new BigNumber(NaN_STATE, null);

    /**
     * Statics for 0 and 1.
     */
    public static final BigNumber BIG_NUMBER_ZERO = BigNumber.fromLong(0);
    public static final BigNumber BIG_NUMBER_ONE = BigNumber.fromLong(1);

}
