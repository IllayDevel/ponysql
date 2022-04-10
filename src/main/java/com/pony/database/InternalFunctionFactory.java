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

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Arrays;
import java.util.Locale;
import java.text.*;
import java.io.InputStream;
import java.io.IOException;

import com.pony.util.Cache;
import com.pony.database.global.SQLTypes;
import com.pony.database.global.CastHelper;
import com.pony.database.global.ByteLongObject;
import com.pony.database.global.BlobAccessor;
import com.pony.database.global.StringObject;
import com.pony.database.global.StringAccessor;
import com.pony.database.global.ObjectTranslator;
import com.pony.database.jdbc.SQLQuery;
import com.pony.util.BigNumber;

/**
 * A FunctionFactory for all internal SQL functions (including aggregate,
 * mathematical, string functions).  This FunctionFactory is registered with
 * the DatabaseSystem during initialization.
 *
 * @author Tobias Downer
 */

final class InternalFunctionFactory extends FunctionFactory {

    /**
     * Registers the function classes with this factory.
     */
    public void init() {

        // Parses a date/time/timestamp string
        addFunction("dateob", TimeDateParser.DateObFunction.class);
        addFunction("timeob", TimeDateParser.TimeObFunction.class);
        addFunction("timestampob", TimeDateParser.TimeStampObFunction.class);
        addFunction("dateformat", TimeDateParser.DateFormatFunction.class);

        // Casting functions
        addFunction("tonumber", ToNumberFunction.class);
        addFunction("sql_cast", SqlCastManager.SQLCastFunction.class);
        // String functions
        addFunction("lower", LowerFunction.class);
        addFunction("upper", UpperFunction.class);
        addFunction("concat", ConcatFunction.class);
        addFunction("length", LengthFunction.class);
        addFunction("substring", SubstringFunction.class);
        addFunction("sql_trim", SQLTrimFunction.class);
        addFunction("ltrim", LTrimFunction.class);
        addFunction("rtrim", RTrimFunction.class);
        // Security
        addFunction("user", UserFunction.class);
        addFunction("privgroups", PrivGroupsFunction.class);
        // Aggregate
        addFunction("count", CountFunction.class, FunctionInfo.AGGREGATE);
        addFunction("distinct_count",
                DistinctCountFunction.class, FunctionInfo.AGGREGATE);
        addFunction("avg", AvgFunction.class, FunctionInfo.AGGREGATE);
        addFunction("sum", SumFunction.class, FunctionInfo.AGGREGATE);
        addFunction("min", MinFunction.class, FunctionInfo.AGGREGATE);
        addFunction("max", MaxFunction.class, FunctionInfo.AGGREGATE);
        addFunction("aggor", AggOrFunction.class, FunctionInfo.AGGREGATE);
        // Mathematical
        addFunction("abs", AbsFunction.class);
        addFunction("sign", SignFunction.class);
        addFunction("mod", ModFunction.class);
        addFunction("round", RoundFunction.class);
        addFunction("pow", PowFunction.class);
        addFunction("sqrt", SqrtFunction.class);
        // Sequence operations
        addFunction("uniquekey",
                UniqueKeyFunction.class, FunctionInfo.STATE_BASED);
        addFunction("nextval",
                NextValFunction.class, FunctionInfo.STATE_BASED);
        addFunction("currval",
                CurrValFunction.class, FunctionInfo.STATE_BASED);
        addFunction("setval",
                SetValFunction.class, FunctionInfo.STATE_BASED);
        // Misc
        addFunction("hextobinary", HexToBinaryFunction.class);
        addFunction("binarytohex", BinaryToHexFunction.class);
        // Lists
        addFunction("least", LeastFunction.class);
        addFunction("greatest", GreatestFunction.class);
        // Branch
        addFunction("if", IfFunction.class);
        addFunction("coalesce", CoalesceFunction.class);

        // Object instantiation (Internal)
        addFunction("_new_JavaObject", JavaObjectInstantiation2.class);

        // Internal functions
        addFunction("i_frule_convert", ForeignRuleConvert.class);
        addFunction("i_sql_type", SQLTypeString.class);
        addFunction("i_view_data", ViewDataConvert.class);
        addFunction("i_privilege_string", PrivilegeString.class);

    }


    // ---------- The internal functions ----------

    // ---------- Grouping functions ----------

    private static class CountFunction extends AbstractFunction {

        public CountFunction(Expression[] params) {
            super("count", params);
            setAggregate(true);

            if (parameterCount() != 1) {
                throw new RuntimeException("'count' function must have one argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            if (group == null) {
                throw new RuntimeException(
                        "'count' can only be used as an aggregate function.");
            }

            int size = group.size();
            TObject result;
            // if, count(*)
            if (size == 0 || isGlob()) {
                result = TObject.intVal(size);
            } else {
                // Otherwise we need to count the number of non-null entries in the
                // columns list(s).

                int total_count = size;

                Expression exp = getParameter(0);
                for (int i = 0; i < size; ++i) {
                    TObject val =
                            exp.evaluate(null, group.getVariableResolver(i), context);
                    if (val.isNull()) {
                        --total_count;
                    }
                }

                result = TObject.intVal(total_count);
            }

            return result;
        }

    }

    // --

    private static class DistinctCountFunction extends AbstractFunction {

        public DistinctCountFunction(Expression[] params) {
            super("distinct_count", params);
            setAggregate(true);

            if (parameterCount() <= 0) {
                throw new RuntimeException(
                        "'distinct_count' function must have at least one argument.");
            }

        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            // There's some issues with implementing this function.
            // For this function to be efficient, we need to have access to the
            // underlying Table object(s) so we can use table indexing to sort the
            // columns.  Otherwise, we will need to keep in memory the group
            // contents so it can be sorted.  Or alternatively (and probably worst
            // of all) don't store in memory, but use an expensive iterative search
            // for non-distinct rows.
            //
            // An iterative search will be terrible for large groups with mostly
            // distinct rows.  But would be okay for large groups with few distinct
            // rows.

            if (group == null) {
                throw new RuntimeException(
                        "'count' can only be used as an aggregate function.");
            }

            final int rows = group.size();
            if (rows <= 1) {
                // If count of entries in group is 0 or 1
                return TObject.intVal(rows);
            }

            // Make an array of all cells in the group that we are finding which
            // are distinct.
            final int cols = parameterCount();
            final TObject[] group_r = new TObject[rows * cols];
            int n = 0;
            for (int i = 0; i < rows; ++i) {
                VariableResolver vr = group.getVariableResolver(i);
                for (int p = 0; p < cols; ++p) {
                    Expression exp = getParameter(p);
                    group_r[n + p] = exp.evaluate(null, vr, context);
                }
                n += cols;
            }

            // A comparator that sorts this set,
            Comparator c = (ob1, ob2) -> {
                int r1 = (Integer) ob1;
                int r2 = (Integer) ob2;

                // Compare row r1 with r2
                int index1 = r1 * cols;
                int index2 = r2 * cols;
                for (int n1 = 0; n1 < cols; ++n1) {
                    int v = group_r[index1 + n1].compareTo(group_r[index2 + n1]);
                    if (v != 0) {
                        return v;
                    }
                }

                // If we got here then rows must be equal.
                return 0;
            };

            // The list of indexes,
            Object[] list = new Object[rows];
            for (int i = 0; i < rows; ++i) {
                list[i] = i;
            }

            // Sort the list,
            Arrays.sort(list, c);

            // The count of distinct elements, (there will always be at least 1)
            int distinct_count = 1;
            for (int i = 1; i < rows; ++i) {
                int v = c.compare(list[i], list[i - 1]);
                // If v == 0 then entry is not distinct with the previous element in
                // the sorted list therefore the distinct counter is not incremented.
                if (v > 0) {
                    // If current entry is greater than previous then we've found a
                    // distinct entry.
                    ++distinct_count;
                } else if (v < 0) {
                    // The current element should never be less if list is sorted in
                    // ascending order.
                    throw new Error("Assertion failed - the distinct list does not " +
                            "appear to be sorted.");
                }
            }

            // If the first entry in the list is NULL then subtract 1 from the
            // distinct count because we shouldn't be counting NULL entries.
            if (list.length > 0) {
                int first_entry = (Integer) list[0];
                // Assume first is null
                boolean first_is_null = true;
                for (int m = 0; m < cols && first_is_null == true; ++m) {
                    TObject val = group_r[(first_entry * cols) + m];
                    if (!val.isNull()) {
                        // First isn't null
                        first_is_null = false;
                    }
                }
                // Is first NULL?
                if (first_is_null) {
                    // decrease distinct count so we don't count the null entry.
                    distinct_count = distinct_count - 1;
                }
            }

            return TObject.intVal(distinct_count);
        }

    }

    // --

    private static class AvgFunction extends AbstractAggregateFunction {

        public AvgFunction(Expression[] params) {
            super("avg", params);
        }

        public TObject evalAggregate(GroupResolver group, QueryContext context,
                                     TObject ob1, TObject ob2) {
            // This will sum,
            if (ob1 != null) {
                if (ob2.isNull()) {
                    return ob1;
                } else {
                    if (!ob1.isNull()) {
                        return ob1.operatorAdd(ob2);
                    } else {
                        return ob2;
                    }
                }
            }
            return ob2;
        }

        public TObject postEvalAggregate(GroupResolver group, QueryContext context,
                                         TObject result) {
            // Find the average from the sum result
            if (result.isNull()) {
                return result;
            }
            return result.operatorDivide(TObject.intVal(group.size()));
        }

    }

    // --

    private static class SumFunction extends AbstractAggregateFunction {

        public SumFunction(Expression[] params) {
            super("sum", params);
        }

        public TObject evalAggregate(GroupResolver group, QueryContext context,
                                     TObject ob1, TObject ob2) {
            // This will sum,
            if (ob1 != null) {
                if (ob2.isNull()) {
                    return ob1;
                } else {
                    if (!ob1.isNull()) {
                        return ob1.operatorAdd(ob2);
                    } else {
                        return ob2;
                    }
                }
            }
            return ob2;
        }

    }

    // --

    private static class MinFunction extends AbstractAggregateFunction {

        public MinFunction(Expression[] params) {
            super("min", params);
        }

        public TObject evalAggregate(GroupResolver group, QueryContext context,
                                     TObject ob1, TObject ob2) {
            // This will find min,
            if (ob1 != null) {
                if (ob2.isNull()) {
                    return ob1;
                } else {
                    if (!ob1.isNull() && ob1.compareToNoNulls(ob2) < 0) {
                        return ob1;
                    } else {
                        return ob2;
                    }
                }
            }
            return ob2;
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            // Set to return the same type object as this variable.
            return getParameter(0).returnTType(resolver, context);
        }

    }

    // --

    private static class MaxFunction extends AbstractAggregateFunction {

        public MaxFunction(Expression[] params) {
            super("max", params);
        }

        public TObject evalAggregate(GroupResolver group, QueryContext context,
                                     TObject ob1, TObject ob2) {
            // This will find max,
            if (ob1 != null) {
                if (ob2.isNull()) {
                    return ob1;
                } else {
                    if (!ob1.isNull() && ob1.compareToNoNulls(ob2) > 0) {
                        return ob1;
                    } else {
                        return ob2;
                    }
                }
            }
            return ob2;
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            // Set to return the same type object as this variable.
            return getParameter(0).returnTType(resolver, context);
        }

    }

    // --

    private static class AggOrFunction extends AbstractAggregateFunction {

        public AggOrFunction(Expression[] params) {
            super("aggor", params);
        }

        public TObject evalAggregate(GroupResolver group, QueryContext context,
                                     TObject ob1, TObject ob2) {
            // Assuming bitmap numbers, this will find the result of or'ing all the
            // values in the aggregate set.
            if (ob1 != null) {
                if (ob2.isNull()) {
                    return ob1;
                } else {
                    if (!ob1.isNull()) {
                        return ob1.operatorOr(ob2);
                    } else {
                        return ob2;
                    }
                }
            }
            return ob2;
        }

    }


    // ---------- User functions ----------

    // Returns the user name
    private static class UserFunction extends AbstractFunction {

        public UserFunction(Expression[] params) {
            super("user", params);

            if (parameterCount() > 0) {
                throw new RuntimeException("'user' function must have no arguments.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            return TObject.stringVal(context.getUserName());
        }

        public TType returnTType() {
            return TType.STRING_TYPE;
        }

    }

    // Returns the comma (",") deliminated priv groups the user belongs to.
    private static class PrivGroupsFunction extends AbstractFunction {

        public PrivGroupsFunction(Expression[] params) {
            super("privgroups", params);

            if (parameterCount() > 0) {
                throw new RuntimeException(
                        "'privgroups' function must have no arguments.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            throw new RuntimeException(
                    "'PrivGroups' function currently not working.");
        }

        public TType returnTType() {
            return TType.STRING_TYPE;
        }

    }


    // ---------- String functions ----------

    private static class LowerFunction extends AbstractFunction {

        public LowerFunction(Expression[] params) {
            super("lower", params);

            if (parameterCount() != 1) {
                throw new RuntimeException("Lower function must have one argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject ob = getParameter(0).evaluate(group, resolver, context);
            if (ob.isNull()) {
                return ob;
            }
            return new TObject(ob.getTType(),
                    ob.getObject().toString().toLowerCase());
        }

        public TType returnTType() {
            return TType.STRING_TYPE;
        }

    }

    // --

    private static class UpperFunction extends AbstractFunction {

        public UpperFunction(Expression[] params) {
            super("upper", params);

            if (parameterCount() != 1) {
                throw new RuntimeException("Upper function must have one argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject ob = getParameter(0).evaluate(group, resolver, context);
            if (ob.isNull()) {
                return ob;
            }
            return new TObject(ob.getTType(),
                    ob.getObject().toString().toUpperCase());
        }

        public TType returnTType() {
            return TType.STRING_TYPE;
        }

    }

    // --

    private static class ConcatFunction extends AbstractFunction {

        public ConcatFunction(Expression[] params) {
            super("concat", params);

            if (parameterCount() < 1) {
                throw new RuntimeException(
                        "Concat function must have at least one argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            StringBuffer cc = new StringBuffer();

            Locale str_locale = null;
            int str_strength = 0;
            int str_decomposition = 0;
            for (int i = 0; i < parameterCount(); ++i) {
                Expression cur_parameter = getParameter(i);
                TObject ob = cur_parameter.evaluate(group, resolver, context);
                if (!ob.isNull()) {
                    cc.append(ob.getObject().toString());
                    TType type = ob.getTType();
                    if (str_locale == null && type instanceof TStringType) {
                        TStringType str_type = (TStringType) type;
                        str_locale = str_type.getLocale();
                        str_strength = str_type.getStrength();
                        str_decomposition = str_type.getDecomposition();
                    }
                } else {
                    return ob;
                }
            }

            // We inherit the locale from the first string parameter with a locale,
            // or use a default STRING_TYPE if no locale found.
            TType type;
            if (str_locale != null) {
                type = new TStringType(SQLTypes.VARCHAR, -1,
                        str_locale, str_strength, str_decomposition);
            } else {
                type = TType.STRING_TYPE;
            }

            return new TObject(type, new String(cc));
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            // Determine the locale of the first string parameter.
            Locale str_locale = null;
            int str_strength = 0;
            int str_decomposition = 0;
            for (int i = 0; i < parameterCount() && str_locale == null; ++i) {
                TType type = getParameter(i).returnTType(resolver, context);
                if (type instanceof TStringType) {
                    TStringType str_type = (TStringType) type;
                    str_locale = str_type.getLocale();
                    str_strength = str_type.getStrength();
                    str_decomposition = str_type.getDecomposition();
                }
            }

            if (str_locale != null) {
                return new TStringType(SQLTypes.VARCHAR, -1,
                        str_locale, str_strength, str_decomposition);
            } else {
                return TType.STRING_TYPE;
            }
        }

    }

    // --

    private static class LengthFunction extends AbstractFunction {

        public LengthFunction(Expression[] params) {
            super("length", params);

            if (parameterCount() != 1) {
                throw new RuntimeException("Length function must have one argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject ob = getParameter(0).evaluate(group, resolver, context);
            if (ob.isNull()) {
                return ob;
            }
            if (ob.getTType() instanceof TBinaryType) {
                BlobAccessor blob = (BlobAccessor) ob.getObject();
                return TObject.intVal(blob.length());
            }
            if (ob.getTType() instanceof TStringType) {
                StringAccessor str = (StringAccessor) ob.getObject();
                return TObject.intVal(str.length());
            }
            return TObject.intVal(ob.getObject().toString().length());
        }

    }

    // --

    private static class SubstringFunction extends AbstractFunction {

        public SubstringFunction(Expression[] params) {
            super("substring", params);

            if (parameterCount() < 1 || parameterCount() > 3) {
                throw new RuntimeException(
                        "Substring function needs one to three arguments.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject ob = getParameter(0).evaluate(group, resolver, context);
            if (ob.isNull()) {
                return ob;
            }
            String str = ob.getObject().toString();
            int pcount = parameterCount();
            int str_length = str.length();
            int arg1 = 1;
            int arg2 = str_length;
            if (pcount >= 2) {
                arg1 = getParameter(1).evaluate(group, resolver,
                        context).toBigNumber().intValue();
            }
            if (pcount >= 3) {
                arg2 = getParameter(2).evaluate(group, resolver,
                        context).toBigNumber().intValue();
//        arg2 = Operator.toNumber(
//               getParameter(2).evaluate(group, resolver, context)).intValue();
            }

            // Make sure this call is safe for all lengths of string.
            if (arg1 < 1) {
                arg1 = 1;
            }
            if (arg1 > str_length) {
                return TObject.stringVal("");
            }
            if (arg2 + arg1 > str_length) {
                arg2 = (str_length - arg1) + 1;
            }
            if (arg2 < 1) {
                return TObject.stringVal("");
            }

            return TObject.stringVal(str.substring(arg1 - 1, (arg1 + arg2) - 1));
        }

        public TType returnTType() {
            return TType.STRING_TYPE;
        }

    }

    // --

    private static class SQLTrimFunction extends AbstractFunction {

        public SQLTrimFunction(Expression[] params) {
            super("sql_trim", params);

//      System.out.println(parameterCount());
            if (parameterCount() != 3) {
                throw new RuntimeException(
                        "SQL Trim function must have three parameters.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            // The type of trim (leading, both, trailing)
            TObject ttype = getParameter(0).evaluate(group, resolver, context);
            // Characters to trim
            TObject cob = getParameter(1).evaluate(group, resolver, context);
            if (cob.isNull()) {
                return cob;
            } else if (ttype.isNull()) {
                return TObject.stringVal((StringObject) null);
            }
            String characters = cob.getObject().toString();
            String ttype_str = ttype.getObject().toString();
            // The content to trim.
            TObject ob = getParameter(2).evaluate(group, resolver, context);
            if (ob.isNull()) {
                return ob;
            }
            String str = ob.getObject().toString();

            int skip = characters.length();
            // Do the trim,
            if (ttype_str.equals("leading") || ttype_str.equals("both")) {
                // Trim from the start.
                int scan = 0;
                while (scan < str.length() &&
                        str.indexOf(characters, scan) == scan) {
                    scan += skip;
                }
                str = str.substring(Math.min(scan, str.length()));
            }
            if (ttype_str.equals("trailing") || ttype_str.equals("both")) {
                // Trim from the end.
                int scan = str.length() - 1;
                int i = str.lastIndexOf(characters, scan);
                while (scan >= 0 && i != -1 && i == scan - skip + 1) {
                    scan -= skip;
                    i = str.lastIndexOf(characters, scan);
                }
                str = str.substring(0, Math.max(0, scan + 1));
            }

            return TObject.stringVal(str);
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TType.STRING_TYPE;
        }

    }

    // --

    private static class LTrimFunction extends AbstractFunction {

        public LTrimFunction(Expression[] params) {
            super("ltrim", params);

            if (parameterCount() != 1) {
                throw new RuntimeException(
                        "ltrim function may only have 1 parameter.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject ob = getParameter(0).evaluate(group, resolver, context);
            if (ob.isNull()) {
                return ob;
            }
            String str = ob.getObject().toString();

            // Do the trim,
            // Trim from the start.
            int scan = 0;
            while (scan < str.length() &&
                    str.indexOf(' ', scan) == scan) {
                scan += 1;
            }
            str = str.substring(Math.min(scan, str.length()));

            return TObject.stringVal(str);
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TType.STRING_TYPE;
        }

    }

    // --

    private static class RTrimFunction extends AbstractFunction {

        public RTrimFunction(Expression[] params) {
            super("rtrim", params);

            if (parameterCount() != 1) {
                throw new RuntimeException(
                        "rtrim function may only have 1 parameter.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject ob = getParameter(0).evaluate(group, resolver, context);
            if (ob.isNull()) {
                return ob;
            }
            String str = ob.getObject().toString();

            // Do the trim,
            // Trim from the end.
            int scan = str.length() - 1;
            int i = str.lastIndexOf(" ", scan);
            while (scan >= 0 && i != -1 && i == scan - 2) {
                scan -= 1;
                i = str.lastIndexOf(" ", scan);
            }
            str = str.substring(0, Math.max(0, scan + 1));

            return TObject.stringVal(str);
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TType.STRING_TYPE;
        }

    }


    // ---------- Mathematical functions ----------

    private static class AbsFunction extends AbstractFunction {

        public AbsFunction(Expression[] params) {
            super("abs", params);

            if (parameterCount() != 1) {
                throw new RuntimeException("Abs function must have one argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject ob = getParameter(0).evaluate(group, resolver, context);
            if (ob.isNull()) {
                return ob;
            }
            BigNumber num = ob.toBigNumber();
            return TObject.bigNumberVal(num.abs());
        }

    }

    // --

    private static class SignFunction extends AbstractFunction {

        public SignFunction(Expression[] params) {
            super("sign", params);

            if (parameterCount() != 1) {
                throw new RuntimeException("Sign function must have one argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject ob = getParameter(0).evaluate(group, resolver, context);
            if (ob.isNull()) {
                return ob;
            }
            BigNumber num = ob.toBigNumber();
            return TObject.intVal(num.signum());
        }

    }

    // --

    private static class ModFunction extends AbstractFunction {

        public ModFunction(Expression[] params) {
            super("mod", params);

            if (parameterCount() != 2) {
                throw new RuntimeException("Mod function must have two arguments.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject ob1 = getParameter(0).evaluate(group, resolver, context);
            TObject ob2 = getParameter(1).evaluate(group, resolver, context);
            if (ob1.isNull()) {
                return ob1;
            } else if (ob2.isNull()) {
                return ob2;
            }

            double v = ob1.toBigNumber().doubleValue();
            double m = ob2.toBigNumber().doubleValue();
            return TObject.doubleVal(v % m);
        }

    }

    // --

    private static class RoundFunction extends AbstractFunction {

        public RoundFunction(Expression[] params) {
            super("round", params);

            if (parameterCount() < 1 || parameterCount() > 2) {
                throw new RuntimeException(
                        "Round function must have one or two arguments.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject ob1 = getParameter(0).evaluate(group, resolver, context);
            if (ob1.isNull()) {
                return ob1;
            }

            BigNumber v = ob1.toBigNumber();
            int d = 0;
            if (parameterCount() == 2) {
                TObject ob2 = getParameter(1).evaluate(group, resolver, context);
                if (ob2.isNull()) {
                    d = 0;
                } else {
                    d = ob2.toBigNumber().intValue();
                }
            }
            return TObject.bigNumberVal(v.setScale(d, BigDecimal.ROUND_HALF_UP));
        }

    }

    // --

    private static class PowFunction extends AbstractFunction {

        public PowFunction(Expression[] params) {
            super("pow", params);

            if (parameterCount() != 2) {
                throw new RuntimeException("Pow function must have two arguments.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject ob1 = getParameter(0).evaluate(group, resolver, context);
            TObject ob2 = getParameter(1).evaluate(group, resolver, context);
            if (ob1.isNull()) {
                return ob1;
            } else if (ob2.isNull()) {
                return ob2;
            }

            double v = ob1.toBigNumber().doubleValue();
            double w = ob2.toBigNumber().doubleValue();
            return TObject.doubleVal(Math.pow(v, w));
        }

    }

    // --

    private static class SqrtFunction extends AbstractFunction {

        public SqrtFunction(Expression[] params) {
            super("sqrt", params);

            if (parameterCount() != 1) {
                throw new RuntimeException("Sqrt function must have one argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject ob = getParameter(0).evaluate(group, resolver, context);
            if (ob.isNull()) {
                return ob;
            }

            return TObject.bigNumberVal(ob.toBigNumber().sqrt());
        }

    }

    // --

    private static class LeastFunction extends AbstractFunction {

        public LeastFunction(Expression[] params) {
            super("least", params);

            if (parameterCount() < 1) {
                throw new RuntimeException(
                        "Least function must have at least 1 argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject least = null;
            for (int i = 0; i < parameterCount(); ++i) {
                TObject ob = getParameter(i).evaluate(group, resolver, context);
                if (ob.isNull()) {
                    return ob;
                }
                if (least == null || ob.compareTo(least) < 0) {
                    least = ob;
                }
            }
            return least;
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return getParameter(0).returnTType(resolver, context);
        }

    }

    // --

    private static class GreatestFunction extends AbstractFunction {

        public GreatestFunction(Expression[] params) {
            super("greatest", params);

            if (parameterCount() < 1) {
                throw new RuntimeException(
                        "Greatest function must have at least 1 argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject great = null;
            for (int i = 0; i < parameterCount(); ++i) {
                TObject ob = getParameter(i).evaluate(group, resolver, context);
                if (ob.isNull()) {
                    return ob;
                }
                if (great == null || ob.compareTo(great) > 0) {
                    great = ob;
                }
            }
            return great;
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return getParameter(0).returnTType(resolver, context);
        }

    }

    // --

    private static class UniqueKeyFunction extends AbstractFunction {

        public UniqueKeyFunction(Expression[] params) {
            super("uniquekey", params);

            // The parameter is the name of the table you want to bring the unique
            // key in from.
            if (parameterCount() != 1) {
                throw new RuntimeException(
                        "'uniquekey' function must have only 1 argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            String str = getParameter(0).evaluate(group, resolver,
                    context).getObject().toString();
            long v = context.nextSequenceValue(str);
            return TObject.longVal(v);
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TType.NUMERIC_TYPE;
        }

    }

    private static class NextValFunction extends AbstractFunction {

        public NextValFunction(Expression[] params) {
            super("nextval", params);

            // The parameter is the name of the table you want to bring the unique
            // key in from.
            if (parameterCount() != 1) {
                throw new RuntimeException(
                        "'nextval' function must have only 1 argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            String str = getParameter(0).evaluate(group, resolver,
                    context).getObject().toString();
            long v = context.nextSequenceValue(str);
            return TObject.longVal(v);
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TType.NUMERIC_TYPE;
        }

    }

    private static class CurrValFunction extends AbstractFunction {

        public CurrValFunction(Expression[] params) {
            super("currval", params);

            // The parameter is the name of the table you want to bring the unique
            // key in from.
            if (parameterCount() != 1) {
                throw new RuntimeException(
                        "'currval' function must have only 1 argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            String str = getParameter(0).evaluate(group, resolver,
                    context).getObject().toString();
            long v = context.currentSequenceValue(str);
            return TObject.longVal(v);
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TType.NUMERIC_TYPE;
        }

    }

    private static class SetValFunction extends AbstractFunction {

        public SetValFunction(Expression[] params) {
            super("setval", params);

            // The parameter is the name of the table you want to bring the unique
            // key in from.
            if (parameterCount() != 2) {
                throw new RuntimeException(
                        "'setval' function must have 2 arguments.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            String str = getParameter(0).evaluate(group, resolver,
                    context).getObject().toString();
            BigNumber num = getParameter(1).evaluate(group, resolver,
                    context).toBigNumber();
            long v = num.longValue();
            context.setSequenceValue(str, v);
            return TObject.longVal(v);
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TType.NUMERIC_TYPE;
        }

    }


    // --

    private static class HexToBinaryFunction extends AbstractFunction {

        public HexToBinaryFunction(Expression[] params) {
            super("hextobinary", params);

            // One parameter - our hex string.
            if (parameterCount() != 1) {
                throw new RuntimeException(
                        "'hextobinary' function must have only 1 argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            String str = getParameter(0).evaluate(group, resolver,
                    context).getObject().toString();

            int str_len = str.length();
            if (str_len == 0) {
                return new TObject(TType.BINARY_TYPE, new ByteLongObject(new byte[0]));
            }
            // We translate the string to a byte array,
            byte[] buf = new byte[(str_len + 1) / 2];
            int index = 0;
            if (buf.length * 2 != str_len) {
                buf[0] = (byte) Character.digit(str.charAt(0), 16);
                ++index;
            }
            int v = 0;
            for (int i = index; i < str_len; i += 2) {
                v = (Character.digit(str.charAt(i), 16) << 4) |
                        (Character.digit(str.charAt(i + 1), 16));
                buf[index] = (byte) (v & 0x0FF);
                ++index;
            }

            return new TObject(TType.BINARY_TYPE, new ByteLongObject(buf));
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TType.BINARY_TYPE;
        }

    }

    // --

    private static class BinaryToHexFunction extends AbstractFunction {

        final static char[] digits = {
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
                'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
                'u', 'v', 'w', 'x', 'y', 'z'
        };

        public BinaryToHexFunction(Expression[] params) {
            super("binarytohex", params);

            // One parameter - our hex string.
            if (parameterCount() != 1) {
                throw new RuntimeException(
                        "'binarytohex' function must have only 1 argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject ob = getParameter(0).evaluate(group, resolver, context);
            if (ob.isNull()) {
                return ob;
            } else if (ob.getTType() instanceof TBinaryType) {
                StringBuilder buf = new StringBuilder();
                BlobAccessor blob = (BlobAccessor) ob.getObject();
                InputStream bin = blob.getInputStream();
                try {
                    int bval = bin.read();
                    while (bval != -1) {
                        buf.append(digits[((bval >> 4) & 0x0F)]);
                        buf.append(digits[(bval & 0x0F)]);
                        bval = bin.read();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException("IO Error: " + e.getMessage());
                }

//        for (int i = 0; i < arr.length; ++i) {
//          buf.append(digits[((arr[i] >> 4) & 0x0F)]);
//          buf.append(digits[(arr[i] & 0x0F)]);
//        }
                return TObject.stringVal(buf.toString());
            } else {
                throw new RuntimeException(
                        "'binarytohex' parameter type is not a binary object.");
            }

        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TType.STRING_TYPE;
        }

    }



    // --

    // Casts the expression to a BigDecimal number.  Useful in conjunction with
    // 'dateob'
    private static class ToNumberFunction extends AbstractFunction {

        public ToNumberFunction(Expression[] params) {
            super("tonumber", params);

            if (parameterCount() != 1) {
                throw new RuntimeException("TONUMBER function must have one argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            // Casts the first parameter to a number
            return getParameter(0).evaluate(group, resolver,
                    context).castTo(TType.NUMERIC_TYPE);
        }

    }

    // --

    // Conditional - IF(a < 0, NULL, a)
    private static class IfFunction extends AbstractFunction {

        public IfFunction(Expression[] params) {
            super("if", params);
            if (parameterCount() != 3) {
                throw new RuntimeException(
                        "IF function must have exactly three arguments.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject res = getParameter(0).evaluate(group, resolver, context);
            if (res.getTType() instanceof TBooleanType) {
                // Does the result equal true?
                if (res.compareTo(TObject.booleanVal(true)) == 0) {
                    // Resolved to true so evaluate the first argument
                    return getParameter(1).evaluate(group, resolver, context);
                } else {
                    // Otherwise result must evaluate to NULL or false, so evaluate
                    // the second parameter
                    return getParameter(2).evaluate(group, resolver, context);
                }
            }
            // Result was not a boolean so return null
            return TObject.nullVal();
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            // It's impossible to know the return type of this function until runtime
            // because either comparator could be returned.  We could assume that
            // both branch expressions result in the same type of object but this
            // currently is not enforced.

            // Returns type of first argument
            TType t1 = getParameter(1).returnTType(resolver, context);
            // This is a hack for null values.  If the first parameter is null
            // then return the type of the second parameter which hopefully isn't
            // also null.
            if (t1 instanceof TNullType) {
                return getParameter(2).returnTType(resolver, context);
            }
            return t1;
        }

    }

    // --

    // Coalesce - COALESCE(address2, CONCAT(city, ', ', state, '  ', zip))
    private static class CoalesceFunction extends AbstractFunction {

        public CoalesceFunction(Expression[] params) {
            super("coalesce", params);
            if (parameterCount() < 1) {
                throw new RuntimeException(
                        "COALESCE function must have at least 1 parameter.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            int count = parameterCount();
            for (int i = 0; i < count - 1; ++i) {
                TObject res = getParameter(i).evaluate(group, resolver, context);
                if (!res.isNull()) {
                    return res;
                }
            }
            return getParameter(count - 1).evaluate(group, resolver, context);
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            // It's impossible to know the return type of this function until runtime
            // because either comparator could be returned.  We could assume that
            // both branch expressions result in the same type of object but this
            // currently is not enforced.

            // Go through each argument until we find the first parameter we can
            // deduce the class of.
            int count = parameterCount();
            for (int i = 0; i < count; ++i) {
                TType t = getParameter(i).returnTType(resolver, context);
                if (!(t instanceof TNullType)) {
                    return t;
                }
            }
            // Can't work it out so return null type
            return TType.NULL_TYPE;
        }

    }


    // --

    // Instantiates a new java object.
    private static class JavaObjectInstantiation extends AbstractFunction {

        public JavaObjectInstantiation(Expression[] params) {
            super("_new_JavaObject", params);

            if (parameterCount() < 1) {
                throw new RuntimeException(
                        "_new_JavaObject function must have one argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            // Resolve the parameters...
            final int arg_len = parameterCount() - 1;
            Object[] args = new Object[arg_len];
            for (int i = 0; i < args.length; ++i) {
                args[i] = getParameter(i + 1).evaluate(group, resolver,
                        context).getObject();
            }
            Object[] casted_args = new Object[arg_len];

            try {
                String clazz = getParameter(0).evaluate(null, resolver,
                        context).getObject().toString();
                Class c = Class.forName(clazz);

                Constructor[] constructs = c.getConstructors();
                // Search for the first constructor that we can use with the given
                // arguments.
                search_constructs:
                for (Constructor construct : constructs) {
                    Class[] construct_args = construct.getParameterTypes();
                    if (construct_args.length == arg_len) {
                        for (int n = 0; n < arg_len; ++n) {
                            // If we are dealing with a primitive,
                            if (construct_args[n].isPrimitive()) {
                                String class_name = construct_args[n].getName();
                                // If the given argument is a number,
                                if (args[n] instanceof Number) {
                                    Number num = (Number) args[n];
                                    switch (class_name) {
                                        case "byte":
                                            casted_args[n] = num.byteValue();
                                            break;
                                        case "char":
                                            casted_args[n] = (char) num.intValue();
                                            break;
                                        case "double":
                                            casted_args[n] = num.doubleValue();
                                            break;
                                        case "float":
                                            casted_args[n] = num.floatValue();
                                            break;
                                        case "int":
                                            casted_args[n] = num.intValue();
                                            break;
                                        case "long":
                                            casted_args[n] = num.longValue();
                                            break;
                                        case "short":
                                            casted_args[n] = num.shortValue();
                                            break;
                                        default:
                                            // Can't cast the primitive type to a number so break,
                                            break search_constructs;
                                    }

                                }
                                // If we are a boolean, we can cast to primitive boolean
                                else if (args[n] instanceof Boolean) {
                                    // If primitive type constructor arg is a boolean also
                                    if (class_name.equals("boolean")) {
                                        casted_args[n] = args[n];
                                    } else {
                                        break search_constructs;
                                    }
                                }
                                // Otherwise we can't cast,
                                else {
                                    break search_constructs;
                                }

                            }
                            // Not a primitive type constructor arg,
                            else {
                                // PENDING: Allow string -> char conversion
                                if (construct_args[n].isInstance(args[n])) {
                                    casted_args[n] = args[n];
                                } else {
                                    break search_constructs;
                                }
                            }
                        }  // for (int n = 0; n < arg_len; ++n)
                        // If we get here, we have a match...
                        Object ob = construct.newInstance(casted_args);
                        ByteLongObject serialized_ob = ObjectTranslator.serialize(ob);
                        return new TObject(new TJavaObjectType(clazz), serialized_ob);
                    }
                }

                throw new RuntimeException(
                        "Unable to find a constructor for '" + clazz +
                                "' that matches given arguments.");

            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Class not found: " + e.getMessage());
            } catch (InstantiationException e) {
                throw new RuntimeException("Instantiation Error: " + e.getMessage());
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Illegal Access Error: " + e.getMessage());
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(
                        "Illegal Argument Error: " + e.getMessage());
            } catch (InvocationTargetException e) {
                throw new RuntimeException(
                        "Invocation Target Error: " + e.getMessage());
            }

        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            String clazz = getParameter(0).evaluate(null, resolver,
                    context).getObject().toString();
            return new TJavaObjectType(clazz);
        }

    }

    // Instantiates a new java object using Jim McBeath's parameter seach
    // algorithm.
    private static class JavaObjectInstantiation2 extends AbstractFunction {

        public JavaObjectInstantiation2(Expression[] params) {
            super("_new_JavaObject", params);

            if (parameterCount() < 1) {
                throw new RuntimeException(
                        "_new_JavaObject function must have one argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            // Resolve the parameters...
            final int arg_len = parameterCount() - 1;
            TObject[] args = new TObject[arg_len];
            for (int i = 0; i < args.length; ++i) {
                args[i] = getParameter(i + 1).evaluate(group, resolver, context);
            }
            Caster.deserializeJavaObjects(args);

            try {
                // Get the class name of the object to be constructed
                String clazz = getParameter(0).evaluate(null, resolver,
                        context).getObject().toString();
                Class c = Class.forName(clazz);
                Constructor[] constructs = c.getConstructors();

                Constructor bestConstructor =
                        Caster.findBestConstructor(constructs, args);
                if (bestConstructor == null) {
                    // Didn't find a match - build a list of class names of the
                    // args so the user knows what we were looking for.
                    String argTypes = Caster.getArgTypesString(args);
                    throw new RuntimeException(
                            "Unable to find a constructor for '" + clazz +
                                    "' that matches given arguments: " + argTypes);
                }
                Object[] casted_args =
                        Caster.castArgsToConstructor(args, bestConstructor);
                // Call the constructor to create the java object
                Object ob = bestConstructor.newInstance(casted_args);
                ByteLongObject serialized_ob = ObjectTranslator.serialize(ob);
                return new TObject(new TJavaObjectType(clazz), serialized_ob);

            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Class not found: " + e.getMessage());
            } catch (InstantiationException e) {
                throw new RuntimeException("Instantiation Error: " + e.getMessage());
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Illegal Access Error: " + e.getMessage());
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(
                        "Illegal Argument Error: " + e.getMessage());
            } catch (InvocationTargetException e) {
                String msg = e.getMessage();
                if (msg == null) {
                    Throwable th = e.getTargetException();
                    if (th != null) {
                        msg = th.getClass().getName() + ": " + th.getMessage();
                    }
                }
                throw new RuntimeException("Invocation Target Error: " + msg);
            }

        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            String clazz = getParameter(0).evaluate(null, resolver,
                    context).getObject().toString();
            return new TJavaObjectType(clazz);
        }

    }

    // --

    // Used in the 'getxxxKeys' methods in DatabaseMetaData to convert the
    // update delete rule of a foreign key to the JDBC short enum.
    private static class ForeignRuleConvert extends AbstractFunction {

        public ForeignRuleConvert(Expression[] params) {
            super("i_frule_convert", params);

            if (parameterCount() != 1) {
                throw new RuntimeException(
                        "i_frule_convert function must have one argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            // The parameter should be a variable reference that is resolved
            TObject ob = getParameter(0).evaluate(group, resolver, context);
            String str = null;
            if (!ob.isNull()) {
                str = ob.getObject().toString();
            }
            int v;
            if (str == null || str.equals("") || str.equals("NO ACTION")) {
                v = java.sql.DatabaseMetaData.importedKeyNoAction;
            } else if (str.equals("CASCADE")) {
                v = java.sql.DatabaseMetaData.importedKeyCascade;
            } else if (str.equals("SET NULL")) {
                v = java.sql.DatabaseMetaData.importedKeySetNull;
            } else if (str.equals("SET DEFAULT")) {
                v = java.sql.DatabaseMetaData.importedKeySetDefault;
            } else if (str.equals("RESTRICT")) {
                v = java.sql.DatabaseMetaData.importedKeyRestrict;
            } else {
                throw new Error("Unrecognised foreign key rule: " + str);
            }
            // Return the correct enumeration
            return TObject.intVal(v);
        }

    }

    // --

    // Used to form an SQL type string that describes the SQL type and any
    // size/scale information together with it.
    private static class SQLTypeString extends AbstractFunction {

        public SQLTypeString(Expression[] params) {
            super("i_sql_type", params);

            if (parameterCount() != 3) {
                throw new RuntimeException(
                        "i_sql_type function must have three arguments.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            // The parameter should be a variable reference that is resolved
            TObject type_string = getParameter(0).evaluate(group, resolver, context);
            TObject type_size = getParameter(1).evaluate(group, resolver, context);
            TObject type_scale = getParameter(2).evaluate(group, resolver, context);

            StringBuffer result_str = new StringBuffer();
            result_str.append(type_string.toString());
            long size = -1;
            long scale = -1;
            if (!type_size.isNull()) {
                size = type_size.toBigNumber().longValue();
            }
            if (!type_scale.isNull()) {
                scale = type_scale.toBigNumber().longValue();
            }

            if (size != -1) {
                result_str.append('(');
                result_str.append(size);
                if (scale != -1) {
                    result_str.append(',');
                    result_str.append(scale);
                }
                result_str.append(')');
            }

            return TObject.stringVal(new String(result_str));
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TType.STRING_TYPE;
        }

    }

    // --

    // Used to convert view data in the system view table to forms that are
    // human understandable.  Useful function for debugging or inspecting views.

    private static class ViewDataConvert extends AbstractFunction {

        public ViewDataConvert(Expression[] params) {
            super("i_view_data", params);

            if (parameterCount() != 2) {
                throw new RuntimeException(
                        "i_sql_type function must have two arguments.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            // Get the parameters.  The first is a string describing the operation.
            // The second is the binary data to process and output the information
            // for.
            TObject command = getParameter(0).evaluate(group, resolver, context);
            TObject data = getParameter(1).evaluate(group, resolver, context);

            String command_str = command.getObject().toString();
            ByteLongObject blob = (ByteLongObject) data.getObject();

            if (command_str.equalsIgnoreCase("referenced tables")) {
                ViewDef view_def = ViewDef.deserializeFromBlob(blob);
                QueryPlanNode node = view_def.getQueryPlanNode();
                ArrayList touched_tables = node.discoverTableNames(new ArrayList());
                StringBuffer buf = new StringBuffer();
                int sz = touched_tables.size();
                for (int i = 0; i < sz; ++i) {
                    buf.append(touched_tables.get(i));
                    if (i < sz - 1) {
                        buf.append(", ");
                    }
                }
                return TObject.stringVal(new String(buf));
            } else if (command_str.equalsIgnoreCase("plan dump")) {
                ViewDef view_def = ViewDef.deserializeFromBlob(blob);
                QueryPlanNode node = view_def.getQueryPlanNode();
                StringBuffer buf = new StringBuffer();
                node.debugString(0, buf);
                return TObject.stringVal(new String(buf));
            } else if (command_str.equalsIgnoreCase("query string")) {
                SQLQuery query = SQLQuery.deserializeFromBlob(blob);
                return TObject.stringVal(query.toString());
            }

            return TObject.nullVal();

        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TType.STRING_TYPE;
        }

    }

    // --

    // Given a priv_bit number (from SYS_INFO.Grant), this will return a
    // text representation of the privilege.

    private static class PrivilegeString extends AbstractFunction {

        public PrivilegeString(Expression[] params) {
            super("i_privilege_string", params);

            if (parameterCount() != 1) {
                throw new RuntimeException(
                        "i_privilege_string function must have one argument.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {
            TObject priv_bit_ob = getParameter(0).evaluate(group, resolver, context);
            int priv_bit = ((BigNumber) priv_bit_ob.getObject()).intValue();
            Privileges privs = new Privileges();
            privs = privs.add(priv_bit);
            return TObject.stringVal(privs.toString());
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TType.STRING_TYPE;
        }

    }


}