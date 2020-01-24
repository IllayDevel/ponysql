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

import java.util.Locale;
import java.text.Collator;

import com.pony.database.global.StringAccessor;

import java.io.Reader;
import java.io.IOException;

/**
 * An implementation of TType for a String.
 *
 * @author Tobias Downer
 */

public final class TStringType extends TType {

    static final long serialVersionUID = -4189752898050725908L;

    /**
     * The maximum allowed size for the string.
     */
    private final int max_size;

    /**
     * The locale of the string.
     */
    private Locale locale;

    /**
     * The strength of the collator for this string (as defined in
     * java.text.Collator).
     */
    private final int strength;

    /**
     * The decomposition mode of the collator for this string type (as defined in
     * java.text.Collator).
     */
    private final int decomposition;

    /**
     * The Collator object for this type, created when we first compare objects.
     */
    private transient Collator collator;

    /**
     * Constructs a type with the given sql_type value, the maximum size,
     * and the locale of the string.  Note that the 'sql_type' MUST be a string
     * SQL type.
     * <p>
     * Note that a string type may be constructed with a NULL locale which
     * means strings are compared lexicographically.
     */
    public TStringType(int sql_type, int max_size, Locale locale,
                       int strength, int decomposition) {
        super(sql_type);
        this.max_size = max_size;
        this.strength = strength;
        this.decomposition = decomposition;
        this.locale = locale;
    }

    /**
     * Constructs a type with the given sql_type value, the maximum size,
     * and the locale of the string.  Note that the 'sql_type' MUST be a string
     * SQL type.
     * <p>
     * Note that a string type may be constructed with a NULL locale which
     * means strings are compared lexicographically.  The string locale is
     * formated as [2 char language][2 char country][rest is variant].  For
     * example, US english would be 'enUS', French would be 'fr' and Germany
     * would be 'deDE'.
     */
    public TStringType(int sql_type, int max_size, String locale_str,
                       int strength, int decomposition) {
        super(sql_type);
        this.max_size = max_size;
        this.strength = strength;
        this.decomposition = decomposition;

        if (locale_str != null && locale_str.length() >= 2) {
            String language = locale_str.substring(0, 2);
            String country = "";
            String variant = "";
            if (locale_str.length() > 2) {
                country = locale_str.substring(2, 4);
                if (locale_str.length() > 4) {
                    variant = locale_str.substring(4);
                }
            }
            locale = new Locale(language, country, variant);
        }

    }

    /**
     * Constructor without strength and decomposition that sets to default
     * levels.
     */
    public TStringType(int sql_type, int max_size, String locale_str) {
        this(sql_type, max_size, locale_str, -1, -1);
    }


    /**
     * Returns the maximum size of the string (-1 is don't care).
     */
    public int getMaximumSize() {
        return max_size;
    }

    /**
     * Returns the strength of this string type as defined in java.text.Collator.
     */
    public int getStrength() {
        return strength;
    }

    /**
     * Returns the decomposition of this string type as defined in
     * java.text.Collator.
     */
    public int getDecomposition() {
        return decomposition;
    }

    /**
     * Returns the locale of the string.
     */
    public Locale getLocale() {
        return locale;
    }

    /**
     * Returns the locale information as a formatted string.
     * <p>
     * Note that a string type may be constructed with a NULL locale which
     * means strings are compared lexicographically.  The string locale is
     * formated as [2 char language][2 char country][rest is variant].  For
     * example, US english would be 'enUS', French would be 'fr' and Germany
     * would be 'deDE'.
     */
    public String getLocaleString() {
        if (locale == null) {
            return "";
        } else {
            StringBuffer locale_str = new StringBuffer();
            locale_str.append(locale.getLanguage());
            locale_str.append(locale.getCountry());
            locale_str.append(locale.getVariant());
            return new String(locale_str);
        }
    }

    /**
     * An implementation of a lexicographical compareTo operation on a
     * StringAccessor object.  This uses the Reader object to compare the strings
     * over a stream if the size is such that it is more efficient to do so.
     */
    private int lexicographicalOrder(StringAccessor str1, StringAccessor str2) {
        // If both strings are small use the 'toString' method to compare the
        // strings.  This saves the overhead of having to store very large string
        // objects in memory for all comparisons.
        long str1_size = str1.length();
        long str2_size = str2.length();
        if (str1_size < 32 * 1024 &&
                str2_size < 32 * 1024) {
            return str1.toString().compareTo(str2.toString());
        }

        // The minimum size
        long size = Math.min(str1_size, str2_size);
        Reader r1 = str1.getReader();
        Reader r2 = str2.getReader();
        try {
            try {
                while (size > 0) {
                    int c1 = r1.read();
                    int c2 = r2.read();
                    if (c1 != c2) {
                        return c1 - c2;
                    }
                    --size;
                }
                // They compare equally up to the limit, so now compare sizes,
                if (str1_size > str2_size) {
                    // If str1 is larger
                    return 1;
                } else if (str1_size < str2_size) {
                    // If str1 is smaller
                    return -1;
                }
                // Must be equal
                return 0;
            } finally {
                r1.close();
                r2.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("IO Error: " + e.getMessage());
        }

    }

    /**
     * Returns the java.text.Collator object for this string type.  This collator
     * is used to compare strings of this locale.
     * <p>
     * This method is synchronized because a side effect of this method is to
     * store the collator object instance in a local variable.
     */
    private synchronized Collator getCollator() {
        if (collator != null) {
            return collator;
        } else {
            // NOTE: Even if we are creating a lot of these objects, it shouldn't
            //   be too bad on memory usage because Collator.getInstance caches
            //   collation information behind the scenes.
            collator = Collator.getInstance(locale);
            int strength = getStrength();
            int decomposition = getStrength();
            if (strength >= 0) {
                collator.setStrength(strength);
            }
            if (decomposition >= 0) {
                collator.setDecomposition(decomposition);
            }
            return collator;
        }
    }

    // ---------- Overwritten from TType ----------

    /**
     * For strings, the locale must be the same for the types to be comparable.
     * If the locale is not the same then they are not comparable.  Note that
     * strings with a locale of null can be compared with any other locale.  So
     * this will only return false if both types have different (but defined)
     * locales.
     */
    public boolean comparableTypes(TType type) {
        // Are we comparing with another string type?
        if (type instanceof TStringType) {
            TStringType s_type = (TStringType) type;
            // If either locale is null return true
            if (getLocale() == null || s_type.getLocale() == null) {
                return true;
            }
            // If the locales are the same return true
            return getLocale().equals(s_type.getLocale());
        }
        return false;
    }

    public int compareObs(Object ob1, Object ob2) {
        if (ob1 == ob2) {
            return 0;
        }
        // If lexicographical ordering,
        if (locale == null) {
            return lexicographicalOrder((StringAccessor) ob1, (StringAccessor) ob2);
//      return ob1.toString().compareTo(ob2.toString());
        } else {
            return getCollator().compare(ob1.toString(), ob2.toString());
        }
    }

    public int calculateApproximateMemoryUse(Object ob) {
        if (ob != null) {
            return (((StringAccessor) ob).length() * 2) + 24;
        } else {
            return 32;
        }
    }

    public Class javaClass() {
        return StringAccessor.class;
    }

}
