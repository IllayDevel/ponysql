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

package com.pony.database;

/**
 * An object that represents a range of values to select from a list.  A range
 * has a start value, an end value, and whether we should pick inclusive or
 * exclusive of the end value.  The start value may be a concrete value from
 * the set or it may be a flag that represents the start or end of the list.
 * <p>
 * For example, to select the first item from a set the range would be;
 * <pre>
 * RANGE:
 *   start = FIRST_VALUE, first
 *   end   = LAST_VALUE, first
 * </pre>
 * To select the last item from a set the range would be;
 * <pre>
 * RANGE:
 *   start = FIRST_VALUE, last
 *   end   = LAST_VALUE, last
 * </pre>
 * To select the range of values between '10' and '15' then range would be;
 * <pre>
 * RANGE:
 *   start = FIRST_VALUE, '10'
 *   end   = LAST_VALUE, '15'
 * </pre>
 * Note that the the start value may not compare less than the end value.  For
 * example, start can not be 'last' and end can not be 'first'.
 *
 * @author Tobias Downer
 */

public final class SelectableRange {

    // ---------- Statics ----------

    /**
     * An object that represents the first value in the set.
     * <p>
     * Note that these objects have no (NULL) type.
     */
    public static final TObject FIRST_IN_SET =
            new TObject(TType.NULL_TYPE, "[FIRST_IN_SET]");

    /**
     * An object that represents the last value in the set.
     * <p>
     * Note that these objects have no (NULL) type.
     */
    public static final TObject LAST_IN_SET =
            new TObject(TType.NULL_TYPE, "[LAST_IN_SET]");

    /**
     * Represents the various points in the set on the value to represent the
     * set range.
     */
    public static final byte FIRST_VALUE = 1,
            LAST_VALUE = 2,
            BEFORE_FIRST_VALUE = 3,
            AFTER_LAST_VALUE = 4;

    // ---------- Members ----------

    /**
     * The start of the range to select from the set.
     */
    private final TObject start;

    /**
     * The end of the range to select from the set.
     */
    private final TObject end;

    /**
     * Denotes the place for the range to start with respect to the start value.
     * Either FIRST_VALUE or AFTER_LAST_VALUE.
     */
    private final byte set_start_flag;

    /**
     * Denotes the place for the range to end with respect to the end value.
     * Either BEFORE_FIRST_VALUE or LAST_VALUE.
     */
    private final byte set_end_flag;

    /**
     * Constructs the range.
     */
    public SelectableRange(byte set_start_flag, TObject start,
                           byte set_end_flag, TObject end) {
        this.start = start;
        this.end = end;
        this.set_start_flag = set_start_flag;
        this.set_end_flag = set_end_flag;
    }

    /**
     * Returns the start of the range.
     * NOTE: This may return FIRST_IN_SET or LAST_IN_SET.
     */
    public TObject getStart() {
        return start;
    }

    /**
     * Returns the end of the range.
     * NOTE: This may return FIRST_IN_SET or LAST_IN_SET.
     */
    public TObject getEnd() {
        return end;
    }

    /**
     * Returns the place for the range to start (either FIRST_VALUE or
     * AFTER_LAST_VALUE)
     */
    public byte getStartFlag() {
        return set_start_flag;
    }

    /**
     * Returns the place for the range to end (either BEFORE_FIRST_VALUE or
     * LAST VALUE).
     */
    public byte getEndFlag() {
        return set_end_flag;
    }


    /**
     * Outputs this range as a string.
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();
        if (getStartFlag() == FIRST_VALUE) {
            buf.append("FIRST_VALUE ");
        } else if (getStartFlag() == AFTER_LAST_VALUE) {
            buf.append("AFTER_LAST_VALUE ");
        }
        buf.append(getStart());
        buf.append(" -> ");
        if (getEndFlag() == LAST_VALUE) {
            buf.append("LAST_VALUE ");
        } else if (getEndFlag() == BEFORE_FIRST_VALUE) {
            buf.append("BEFORE_FIRST_VALUE ");
        }
        buf.append(getEnd());
        return new String(buf);
    }

    /**
     * Returns true if this range is equal to the given range.
     */
    public boolean equals(Object ob) {
        if (super.equals(ob)) {
            return true;
        }

        SelectableRange dest_range = (SelectableRange) ob;
        return (getStart().valuesEqual(dest_range.getStart()) &&
                getEnd().valuesEqual(dest_range.getEnd()) &&
                getStartFlag() == dest_range.getStartFlag() &&
                getEndFlag() == dest_range.getEndFlag());
    }

    // ---------- Statics ----------

    /**
     * The range that represents the entire range (including null).
     */
    public static final SelectableRange FULL_RANGE =
            new SelectableRange(FIRST_VALUE, FIRST_IN_SET, LAST_VALUE, LAST_IN_SET);

    /**
     * The range that represents the entire range (not including null).
     */
    public static final SelectableRange FULL_RANGE_NO_NULLS =
            new SelectableRange(AFTER_LAST_VALUE, TObject.nullVal(),
                    LAST_VALUE, LAST_IN_SET);


}
