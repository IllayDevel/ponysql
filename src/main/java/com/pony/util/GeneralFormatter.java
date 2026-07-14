/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

package com.pony.util;

import java.math.BigDecimal;

/**
 * This class provides several static convenience functions for formatting
 * various types of information such as a time frame.
 * <p>
 * @author Tobias Downer
 */

public class GeneralFormatter {

    /**
     * These statics represent switches for the visual formatting of the
     * time frame.  It is desirable to have the time frame represented in
     * different denominations.
     */
    public static final int MAX_WEEKS = 0;
    public static final int MAX_DAYS = 1;
    public static final int MAX_HOURS = 2;
    public static final int MAX_MINUTES = 3;
    public static final int MAX_SECONDS = 4;
    public static final int MAX_MILLISECONDS = 5;

    /**
     * These statics represent some information about how many milliseconds are
     * in various measures of time.
     */
    private static final long MILLIS_IN_WEEK = 7 * 24 * 60 * 60 * 1000;
    private static final long MILLIS_IN_DAY = 24 * 60 * 60 * 1000;
    private static final long MILLIS_IN_HOUR = 60 * 60 * 1000;
    private static final long MILLIS_IN_MINUTE = 60 * 1000;
    private static final long MILLIS_IN_SECOND = 1000;

    /**
     * Appends a frame of time onto the given StringBuffer.  This is used to
     * construct a string representing the current time frame.
     */
    private static void appendFrame(StringBuffer str, double num, String frame,
                                    boolean do_round, boolean append_plural_s) {
        // Should we round the number?  (remove the decimal part)
        if (do_round) {
            num = (long) num;
        }
        // Don't bother printing 0 length time frames
        if (num != 0) {
            str.append(new BigDecimal(num));
            str.append(' ');
            str.append(frame);
            // Append 's' on the end to represent plurals for all except 1 unit
            if (num != 1 && append_plural_s) {
                str.append('s');
            }
            str.append(' ');
        }
    }

    /**
     * Appends time frame representation information into the given StringBuffer
     * for various types of visual time frame formats.
     */
    public static void appendWeekType(StringBuffer str, double total,
                                      boolean shorthand) {
        double num;
        // Total number of weeks
        num = total / MILLIS_IN_WEEK;
        appendFrame(str, num, "week", true, true);
        // Total number of days
        num = (total % MILLIS_IN_WEEK) / MILLIS_IN_DAY;
        appendFrame(str, num, "day", true, true);
        // Total number of hours
        num = (total % MILLIS_IN_DAY) / MILLIS_IN_HOUR;
        appendFrame(str, num, "hr", true, true);
        // Total number of minutes
        num = (total % MILLIS_IN_HOUR) / MILLIS_IN_MINUTE;
        appendFrame(str, num, "min", true, true);
        // Total number of seconds
        num = (total % MILLIS_IN_MINUTE) / MILLIS_IN_SECOND;
        appendFrame(str, num, "sec", true, false);
        // Total number of milliseconds
        num = total % MILLIS_IN_SECOND;
        appendFrame(str, num, "ms", true, false);
    }

    public static void appendDayType(StringBuffer str, double total,
                                     boolean shorthand) {
        double num;
        // Total number of days
        num = total / MILLIS_IN_DAY;
        appendFrame(str, num, "day", true, true);
        // Total number of hours
        num = (total % MILLIS_IN_DAY) / MILLIS_IN_HOUR;
        appendFrame(str, num, "hr", true, true);
        // Total number of minutes
        num = (total % MILLIS_IN_HOUR) / MILLIS_IN_MINUTE;
        appendFrame(str, num, "min", true, true);
        // Total number of seconds
        num = (total % MILLIS_IN_MINUTE) / MILLIS_IN_SECOND;
        appendFrame(str, num, "sec", true, false);
        // Total number of milliseconds
        num = total % MILLIS_IN_SECOND;
        appendFrame(str, num, "ms", true, false);
    }

    public static void appendHourType(StringBuffer str, double total,
                                      boolean shorthand) {
        double num;
        // Total number of hours
        num = total / MILLIS_IN_HOUR;
        appendFrame(str, num, "hr", true, true);
        // Total number of minutes
        num = (total % MILLIS_IN_HOUR) / MILLIS_IN_MINUTE;
        appendFrame(str, num, "min", true, true);
        // Total number of seconds
        num = (total % MILLIS_IN_MINUTE) / MILLIS_IN_SECOND;
        appendFrame(str, num, "sec", true, false);
        // Total number of milliseconds
        num = total % MILLIS_IN_SECOND;
        appendFrame(str, num, "ms", true, false);
    }

    public static void appendMinuteType(StringBuffer str, double total,
                                        boolean shorthand) {
        double num;
        // Total number of minutes
        num = total / MILLIS_IN_MINUTE;
        appendFrame(str, num, "min", true, true);
        // Total number of seconds
        num = (total % MILLIS_IN_MINUTE) / MILLIS_IN_SECOND;
        appendFrame(str, num, "sec", true, false);
        // Total number of milliseconds
        num = total % MILLIS_IN_SECOND;
        appendFrame(str, num, "ms", true, false);
    }


}
