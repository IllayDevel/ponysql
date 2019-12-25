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

import java.text.CharacterIterator;
import java.text.ParseException;
import java.math.BigDecimal;

/**
 * This class provides several static convenience functions for parsing
 * various types of character sequences.  In most cases, we use a
 * CharacterIterator to represent the sequence of characters being parsed.
 * <p>
 * @author Tobias Downer
 */

public class GeneralParser {

    /**
     * These statics represent some information about how many milliseconds are
     * in various measures of time.
     */
    private static final BigDecimal MILLIS_IN_WEEK =
            new BigDecimal(7 * 24 * 60 * 60 * 1000);
    private static final BigDecimal MILLIS_IN_DAY =
            new BigDecimal(24 * 60 * 60 * 1000);
    private static final BigDecimal MILLIS_IN_HOUR =
            new BigDecimal(60 * 60 * 1000);
    private static final BigDecimal MILLIS_IN_MINUTE =
            new BigDecimal(60 * 1000);
    private static final BigDecimal MILLIS_IN_SECOND =
            new BigDecimal(1000);

    /**
     * Parses a string of 0 or more digits and appends the digits into the string
     * buffer.
     */
    public static void parseDigitString(CharacterIterator i, StringBuffer digit_str) {
        char c = i.current();
        while (Character.isDigit(c)) {
            digit_str.append(c);
            c = i.next();
        }
    }

    /**
     * Parses a string of 0 or more words and appends the characters into the
     * string buffer.
     */
    public static void parseWordString(CharacterIterator i,
                                       StringBuffer word_buffer) {
        char c = i.current();
        while (Character.isLetter(c)) {
            word_buffer.append(c);
            c = i.next();
        }
    }

    /**
     * Moves the iterator past any white space.  White space is ' ', '\t', '\n'
     * and '\r'.
     */
    public static void skipWhiteSpace(CharacterIterator i) {
        char c = i.current();
        while (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
            c = i.next();
        }
    }

    /**
     * This assumes there is a decimal number waiting on the iterator.  It
     * parses the decimal and returns the BigDecimal representation.  It throws
     * a GeneralParseException if we are unable to parse the decimal.
     */
    public static BigDecimal parseBigDecimal(CharacterIterator i)
            throws ParseException {
        boolean done_decimal = false;
        StringBuffer str_val = new StringBuffer();

        // We can start with a '-'
        char c = i.current();
        if (c == '-') {
            str_val.append(c);
            c = i.next();
        }
        // We can start or follow with a '.'
        if (c == '.') {
            done_decimal = true;
            str_val.append(c);
            c = i.next();
        }
        // We must be able to parse a digit
        if (!Character.isDigit(c)) {
            throw new ParseException("Parsing BigDecimal", i.getIndex());
        }
        // Parse the digit string
        parseDigitString(i, str_val);
        // Is there a decimal part?
        c = i.current();
        if (!done_decimal && c == '.') {
            str_val.append(c);
            c = i.next();
            parseDigitString(i, str_val);
        }

        return new BigDecimal(new String(str_val));
    }

    /**
     * Parses a time grammer waiting on the character iterator.  The grammer is
     * quite simple.  It allows for us to specify quite precisely some unit of
     * time measure and convert it to a Java understandable form.  It returns the
     * number of milliseconds that the unit of time represents.
     * For example, the string '2.5 hours' would return:
     *   2.5 hours * 60 minutes * 60 seconds * 1000 milliseconds = 9000000
     * <p>
     * To construct a valid time measure, you must supply a sequence of time
     * measurements.  The valid time measurements are 'week(s)', 'day(s)',
     * 'hour(s)', 'minute(s)', 'second(s)', 'millisecond(s)'.  To construct a
     * time, we simply concatinate the measurements together.  For example,
     *   '3 days 22 hours 9.5 minutes'
     * <p>
     * It accepts any number of time measurements, but not duplicates of the
     * same.
     * <p>
     * The time measures are case insensitive.  It is a little lazy how it reads
     * the grammer.  We could for example enter '1 hours 40 second' or even
     * more extreme, '1 houraboutit 90 secondilianit' both of which are
     * acceptable!
     * <p>
     * This method will keep on parsing the string until the end of the iterator
     * is reached or a non-numeric time measure is found.  It throws a
     * ParseException if an invalid time measure is found or a number is invalid
     * (eg. -3 days).
     * <p>
     * LOCALE ISSUE: This will likely be a difficult method to localise.
     */
    public static BigDecimal parseTimeMeasure(CharacterIterator i)
            throws ParseException {
        boolean time_measured = false;
        BigDecimal time_measure = new BigDecimal(0);
        boolean[] time_parsed = new boolean[6];
        StringBuffer word_buffer = new StringBuffer();
        BigDecimal num;

        while (true) {
            // Parse the number
            skipWhiteSpace(i);
            try {
                num = parseBigDecimal(i);
            } catch (ParseException e) {
                // If we can't parse a number, then return with the current time if
                // any time has been parsed.
                if (time_measured) {
                    return time_measure;
                } else {
                    throw new ParseException("No time value found", i.getIndex());
                }
            }
            if (num.signum() < 0) {
                throw new ParseException("Invalid time value: " + num, i.getIndex());
            }

            skipWhiteSpace(i);

            // Parse the time measure
            word_buffer.setLength(0);
            parseWordString(i, word_buffer);

            String str = new String(word_buffer).toLowerCase();
            if ((str.startsWith("week") ||
                    str.equals("w")) &&
                    !time_parsed[0]) {
                time_measure = time_measure.add(num.multiply(MILLIS_IN_WEEK));
                time_parsed[0] = true;
            } else if ((str.startsWith("day") ||
                    str.equals("d")) &&
                    !time_parsed[1]) {
                time_measure = time_measure.add(num.multiply(MILLIS_IN_DAY));
                time_parsed[1] = true;
            } else if ((str.startsWith("hour") ||
                    str.startsWith("hr") ||
                    str.equals("h")) &&
                    !time_parsed[2]) {
                time_measure = time_measure.add(num.multiply(MILLIS_IN_HOUR));
                time_parsed[2] = true;
            } else if ((str.startsWith("minute") ||
                    str.startsWith("min") ||
                    str.equals("m")) &&
                    !time_parsed[3]) {
                time_measure = time_measure.add(num.multiply(MILLIS_IN_MINUTE));
                time_parsed[3] = true;
            } else if ((str.startsWith("second") ||
                    str.startsWith("sec") ||
                    str.equals("s")) &&
                    !time_parsed[4]) {
                time_measure = time_measure.add(num.multiply(MILLIS_IN_SECOND));
                time_parsed[4] = true;
            } else if ((str.startsWith("millisecond") ||
                    str.equals("ms")) &&
                    !time_parsed[5]) {
                time_measure = time_measure.add(num);
                time_parsed[5] = true;
            } else {
                throw new ParseException("Unknown time measure: " + str, i.getIndex());
            }
            time_measured = true;

        }

    }

}
