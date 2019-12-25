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

import java.util.ArrayList;
import java.util.ListIterator;

/**
 * Represents a complex normalized range of a list.  This is essentially a
 * set of SelectableRange objects that make up a complex view of a range.  For
 * example, say we had a query
 * '(a &gt; 10 and a &lt; 20 and a &lt;&gt; 15) or a &gt;= 50',
 * we could represent this range by the following range set;
 * <p><pre>
 * RANGE: AFTER_LAST_VALUE 10, BEFORE_FIRST_VALUE 15
 * RANGE: AFTER_LAST_VALUE 15, BEFORE_FIRST_VALUE 20
 * RANGE: FIRST_VALUE 50, LAST_VALUE LAST_IN_SET
 * </pre><p>
 * The range is constructed by calls to 'intersect', and 'union'.
 *
 * @author Tobias Downer
 */

public final class SelectableRangeSet {

    /**
     * The list of ranges.
     */
    private final ArrayList range_set;

    /**
     * Constructs the SelectableRangeSet to a full range (a range that encompases
     * all values).  If 'no_nulls' is true then the range can't include null
     * values.
     */
    public SelectableRangeSet() {
        range_set = new ArrayList();
        range_set.add(SelectableRange.FULL_RANGE);
    }

    /**
     * Intersects the given SelectableRange object with the given Operator and
     * value constraint.
     * <p>
     * NOTE: This does not work with the '<>' operator which must be handled
     *   another way.
     */
    private static SelectableRange intersectRange(SelectableRange range,
                                                  Operator op, TObject val, boolean null_check) {
        TObject start = range.getStart();
        byte start_flag = range.getStartFlag();
        TObject end = range.getEnd();
        byte end_flag = range.getEndFlag();

        boolean inclusive = op.is("is") || op.is("=") ||
                op.is(">=") || op.is("<=");

        if (op.is("is") || op.is("=") || op.is(">") || op.is(">=")) {
            // With this operator, NULL values must return null.
            if (null_check && val.isNull()) {
                return null;
            }

            if (start == SelectableRange.FIRST_IN_SET) {
                start = val;
                start_flag = inclusive ? SelectableRange.FIRST_VALUE :
                        SelectableRange.AFTER_LAST_VALUE;
            } else {
                int c = val.compareTo(start);
                if ((c == 0 && start_flag == SelectableRange.FIRST_VALUE) || c > 0) {
                    start = val;
                    start_flag = inclusive ? SelectableRange.FIRST_VALUE :
                            SelectableRange.AFTER_LAST_VALUE;
                }
            }
        }
        if (op.is("is") || op.is("=") || op.is("<") || op.is("<=")) {
            // With this operator, NULL values must return null.
            if (null_check && val.isNull()) {
                return null;
            }

            // If start is first in set, then we have to change it to after NULL
            if (null_check && start == SelectableRange.FIRST_IN_SET) {
                start = TObject.nullVal();
                start_flag = SelectableRange.AFTER_LAST_VALUE;
            }

            if (end == SelectableRange.LAST_IN_SET) {
                end = val;
                end_flag = inclusive ? SelectableRange.LAST_VALUE :
                        SelectableRange.BEFORE_FIRST_VALUE;
            } else {
                int c = val.compareTo(end);
                if ((c == 0 && end_flag == SelectableRange.LAST_VALUE) || c < 0) {
                    end = val;
                    end_flag = inclusive ? SelectableRange.LAST_VALUE :
                            SelectableRange.BEFORE_FIRST_VALUE;
                }
            }
        }

        // If start and end are not null types (if either are, then it means it
        // is a placeholder value meaning start or end of set).
        if (start != SelectableRange.FIRST_IN_SET &&
                end != SelectableRange.LAST_IN_SET) {
            // If start is higher than end, return null
            int c = start.compareTo(end);
            if ((c == 0 && (start_flag == SelectableRange.AFTER_LAST_VALUE ||
                    end_flag == SelectableRange.BEFORE_FIRST_VALUE)) ||
                    c > 0) {
                return null;
            }
        }

        // The new intersected range
        return new SelectableRange(start_flag, start, end_flag, end);
    }

    /**
     * Returns true if the two SelectableRange ranges intersect.
     */
    private static boolean rangeIntersectedBy(SelectableRange range1,
                                              SelectableRange range2) {
        byte start_flag_1 = range1.getStartFlag();
        TObject start_1 = range1.getStart();
        byte end_flag_1 = range1.getEndFlag();
        TObject end_1 = range1.getEnd();

        byte start_flag_2 = range2.getStartFlag();
        TObject start_2 = range2.getStart();
        byte end_flag_2 = range2.getEndFlag();
        TObject end_2 = range2.getEnd();

        TObject start_cell_1, end_cell_1;
        TObject start_cell_2, end_cell_2;

        start_cell_1 = start_1 == SelectableRange.FIRST_IN_SET ? null : start_1;
        end_cell_1 = end_1 == SelectableRange.LAST_IN_SET ? null : end_1;
        start_cell_2 = start_2 == SelectableRange.FIRST_IN_SET ? null : start_2;
        end_cell_2 = end_2 == SelectableRange.LAST_IN_SET ? null : end_2;

        boolean intersect_1 = false;
        if (start_cell_1 != null && end_cell_2 != null) {
            int c = start_cell_1.compareTo(end_cell_2);
            if (c < 0 ||
                    (c == 0 && (start_flag_1 == SelectableRange.FIRST_VALUE ||
                            end_flag_2 == SelectableRange.LAST_VALUE))) {
                intersect_1 = true;
            }
        } else {
            intersect_1 = true;
        }

        boolean intersect_2 = false;
        if (start_cell_2 != null && end_cell_1 != null) {
            int c = start_cell_2.compareTo(end_cell_1);
            if (c < 0 ||
                    (c == 0 && (start_flag_2 == SelectableRange.FIRST_VALUE ||
                            end_flag_1 == SelectableRange.LAST_VALUE))) {
                intersect_2 = true;
            }
        } else {
            intersect_2 = true;
        }

        return (intersect_1 && intersect_2);
    }

    /**
     * Alters the first range so it encompasses the second range.  This assumes
     * that range1 intersects range2.
     */
    private static SelectableRange changeRangeSizeToEncompass(
            SelectableRange range1, SelectableRange range2) {

        byte start_flag_1 = range1.getStartFlag();
        TObject start_1 = range1.getStart();
        byte end_flag_1 = range1.getEndFlag();
        TObject end_1 = range1.getEnd();

        byte start_flag_2 = range2.getStartFlag();
        TObject start_2 = range2.getStart();
        byte end_flag_2 = range2.getEndFlag();
        TObject end_2 = range2.getEnd();

        if (start_1 != SelectableRange.FIRST_IN_SET) {
            if (start_2 != SelectableRange.FIRST_IN_SET) {
                TObject cell = start_1;
                int c = cell.compareTo(start_2);
                if (c > 0 ||
                        c == 0 && start_flag_1 == SelectableRange.AFTER_LAST_VALUE &&
                                start_flag_2 == SelectableRange.FIRST_VALUE) {
                    start_1 = start_2;
                    start_flag_1 = start_flag_2;
                }
            } else {
                start_1 = start_2;
                start_flag_1 = start_flag_2;
            }
        }

        if (end_1 != SelectableRange.LAST_IN_SET) {
            if (end_2 != SelectableRange.LAST_IN_SET) {
                TObject cell = (TObject) end_1;
                int c = cell.compareTo(end_2);
                if (c < 0 ||
                        c == 0 && end_flag_1 == SelectableRange.BEFORE_FIRST_VALUE &&
                                end_flag_2 == SelectableRange.LAST_VALUE) {
                    end_1 = end_2;
                    end_flag_1 = end_flag_2;
                }
            } else {
                end_1 = end_2;
                end_flag_1 = end_flag_2;
            }
        }

        return new SelectableRange(start_flag_1, start_1, end_flag_1, end_1);
    }

    /**
     * Intersects this range with the given Operator and value constraint.
     * For example, if a range is 'a' -> [END] and the given operator is '<=' and
     * the value is 'z' the result range is 'a' -> 'z'.
     */
    public void intersect(Operator op, TObject val) {
        int sz = range_set.size();
        ListIterator i = range_set.listIterator();

        if (op.is("<>") || op.is("is not")) {

            boolean null_check = op.is("<>");

            while (i.hasNext()) {
                SelectableRange range = (SelectableRange) i.next();
                SelectableRange left_range =
                        intersectRange(range, Operator.get("<"), val, null_check);
                SelectableRange right_range =
                        intersectRange(range, Operator.get(">"), val, null_check);
                i.remove();
                if (left_range != null) {
                    i.add(left_range);
                }
                if (right_range != null) {
                    i.add(right_range);
                }
            }

        } else {

            boolean null_check = !op.is("is");

            while (i.hasNext()) {
                SelectableRange range = (SelectableRange) i.next();
                range = intersectRange(range, op, val, null_check);
                if (range == null) {
                    i.remove();
                } else {
                    i.set(range);
                }
            }

        }

    }

    /**
     * Unions this range with the given Operator and value constraint.
     */
    public void union(Operator op, TObject val) {
        throw new Error("PENDING");
    }

    /**
     * Unions the current range set with the given range set.
     */
    public void union(SelectableRangeSet union_to) {
        ArrayList input_set = union_to.range_set;

        int in_sz = input_set.size();
        for (int n = 0; n < in_sz; ++n) {
            // The range to merge in.
            SelectableRange in_range = (SelectableRange) input_set.get(n);

            // For each range in this set
            int sz = range_set.size();
            ListIterator i = range_set.listIterator();
            while (i.hasNext()) {
                SelectableRange range = (SelectableRange) i.next();
                if (rangeIntersectedBy(in_range, range)) {
                    i.remove();
                    in_range = changeRangeSizeToEncompass(in_range, range);
                }
            }

            // Insert into sorted position
            byte start_flag = in_range.getStartFlag();
            TObject start = in_range.getStart();
            byte end_flag = in_range.getEndFlag();
            TObject end = in_range.getEnd();

            if (start == SelectableRange.FIRST_IN_SET) {
                range_set.add(0, in_range);
            } else {
                TObject start_cell = start;
                i = range_set.listIterator();
                while (i.hasNext()) {
                    SelectableRange range = (SelectableRange) i.next();
                    TObject cur_start = range.getStart();
                    if (cur_start != SelectableRange.FIRST_IN_SET) {
                        if (cur_start.compareTo(start_cell) > 0) {
                            i.previous();
                            break;
                        }
                    }
                }
                i.add(in_range);
            }

        }

    }

    /**
     * Returns the range as an array of SelectableRange or an empty array if
     * there is no range.
     */
    public SelectableRange[] toSelectableRangeArray() {
        int sz = range_set.size();
        SelectableRange[] ranges = new SelectableRange[sz];
        for (int i = 0; i < sz; ++i) {
            ranges[i] = (SelectableRange) range_set.get(i);
        }
        return ranges;
    }


    /**
     * Outputs this range as a string, for diagnostic and testing purposes.
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();
        if (range_set.size() == 0) {
            return "(NO RANGE)";
        }
        for (int i = 0; i < range_set.size(); ++i) {
            buf.append(range_set.get(i));
            buf.append(", ");
        }
        return new String(buf);
    }


    /**
     * A test application.
     */
    public static void main(String[] args) {

        TType ttype = TType.STRING_TYPE;

        SelectableRangeSet range_set = new SelectableRangeSet();
        System.out.println(range_set);
        range_set.intersect(Operator.get(">="), new TObject(ttype, "2"));
        System.out.println(range_set);
        range_set.intersect(Operator.get("<>"), new TObject(ttype, "4"));
        System.out.println(range_set);
        range_set.intersect(Operator.get("<>"), new TObject(ttype, "2"));
        System.out.println(range_set);
        range_set.intersect(Operator.get("<>"), new TObject(ttype, "3"));
        System.out.println(range_set);
        range_set.intersect(Operator.get("<>"), new TObject(ttype, "2"));
        System.out.println(range_set);
        range_set.intersect(Operator.get("<>"), new TObject(ttype, "1"));
        System.out.println(range_set);
        range_set.intersect(Operator.get(">="), new TObject(ttype, "3"));
        System.out.println(range_set);
        range_set.intersect(Operator.get("<="), new TObject(ttype, "5"));
        System.out.println(range_set);
        range_set.intersect(Operator.get("<"), new TObject(ttype, "5"));
        System.out.println(range_set);
        range_set.intersect(Operator.get(">="), new TObject(ttype, "6"));
        System.out.println(range_set);

        System.out.println("---");
        SelectableRangeSet range1 = new SelectableRangeSet();
        range1.intersect(Operator.get("="), new TObject(ttype, "k"));
        SelectableRangeSet range2 = new SelectableRangeSet();
        range2.intersect(Operator.get("<>"), new TObject(ttype, "d"));
        range2.intersect(Operator.get("<"), new TObject(ttype, "g"));
        SelectableRangeSet range3 = new SelectableRangeSet();
        range3.intersect(Operator.get(">"), new TObject(ttype, "o"));
        range2.union(range3);
        range1.union(range2);
        System.out.println(range1);

    }

}
