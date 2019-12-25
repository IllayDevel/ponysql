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
import java.util.Date;
import java.text.StringCharacterIterator;

/**
 * An immutable object that represents a frame of time down to the
 * accuracy of a millisecond.
 * <p>
 * This object wraps around a BigDecimal that represents the number of
 * milliseconds it takes to pass through the period.
 *
 * @author Tobias Downer
 */

public class TimeFrame {

    private static final BigDecimal BD_ZERO = new BigDecimal(0);

    /**
     * Formatting enum.
     */
    public static final int WEEKS = 1;
    public static final int DAYS = 2;
    public static final int HOURS = 3;
    public static final int MINUTES = 4;

    /**
     * A BigDecimal that represents the number of milliseconds the time frame
     * represents.
     */
    private final BigDecimal period;

    /**
     * Constructs the TimeFrame for the given time.
     */
    public TimeFrame(BigDecimal period) {
        this.period = period;
    }

    /**
     * Returns the number of milliseconds for the period of this time frame.
     */
    public BigDecimal getPeriod() {
        return period;
    }

    /**
     * Returns true if this time frame represents no time.
     */
    public boolean isNoTime() {
        return period.equals(BD_ZERO);
    }

    /**
     * Returns a Date that is the addition of this period of time to the given
     * date.
     */
    public Date addToDate(Date date) {
        return new Date(date.getTime() + period.longValue());
    }

    /**
     * Returns a string that represents this time frame formatted as a string.
     * The period is formatted as short hand.
     *
     * @param format_type either WEEKS, HOURS, MINUTES
     */
    public String format(int format_type) {
        return format(format_type, true);
    }

    /**
     * Returns a string that represents this time frame formatted as a string.
     *
     * @param format_type either WEEKS, HOURS, MINUTES
     * @param shorthand if false then timeframe is formatted in long hand.
     *   'ms' -> 'milliseconds'
     */
    public String format(int format_type, boolean shorthand) {
        if (period == null) {
            return "";
        }
        StringBuffer str = new StringBuffer();
        double val = period.longValue();
        if (format_type == WEEKS) {
            GeneralFormatter.appendWeekType(str, val, shorthand);
        } else if (format_type == DAYS) {
            GeneralFormatter.appendDayType(str, val, shorthand);
        } else if (format_type == HOURS) {
            GeneralFormatter.appendHourType(str, val, shorthand);
        } else if (format_type == MINUTES) {
            GeneralFormatter.appendMinuteType(str, val, shorthand);
        }
        return str.toString();
    }

    /**
     * Parses the given String and returns a TimeFrame object that represents
     * the date.  This excepts strings such as:
     * <p><pre>
     *   "3 wks 12 days", "5.4 days", "9d", "12 minutes", "24 mins", etc.
     * </pre>
     * <p>
     * See 'GeneralParser' for more details.
     */
    public static TimeFrame parse(String str) throws java.text.ParseException {
        // The 'null' case.
        if (str == null || str.equals("")) {
            return null;
        }

        BigDecimal period =
                GeneralParser.parseTimeMeasure(new StringCharacterIterator(str));
        return new TimeFrame(period);
    }

    /**
     * Returns true if the TimeFrame is equal to another.
     */
    public boolean equals(Object ob) {
        TimeFrame tf = (TimeFrame) ob;
        if (tf == null) {
            return false;
        }
        return (this == tf || period.equals(tf.period));
    }

    /**
     * For Debugging.
     */
    public String toString() {
        return format(WEEKS);
    }

}
