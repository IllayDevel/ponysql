package com.pony.database;

import com.pony.database.global.CastHelper;
import com.pony.database.global.SQLTypes;
import com.pony.util.BigNumber;
import com.pony.util.Cache;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeDateParser {
    static class DateObFunction extends AbstractFunction {

        private final static TType DATE_TYPE = new TDateType(SQLTypes.DATE);

        /**
         * The date format object that handles the conversion of Date objects to a
         * string readable representation of the given date.
         * <p>
         * NOTE: Due to bad design these objects are not thread-safe.
         */
        private final static DateFormat date_format_sho;
        private final static DateFormat date_format_sql;
        private final static DateFormat date_format_med;
        private final static DateFormat date_format_lon;
        private final static DateFormat date_format_ful;

        static {
            date_format_med = DateFormat.getDateInstance(DateFormat.MEDIUM);
            date_format_sho = DateFormat.getDateInstance(DateFormat.SHORT);
            date_format_lon = DateFormat.getDateInstance(DateFormat.LONG);
            date_format_ful = DateFormat.getDateInstance(DateFormat.FULL);

            // The SQL date format
            date_format_sql = new SimpleDateFormat("yyyy-MM-dd");
        }

        private static TObject dateVal(Date d) {
            return new TObject(DATE_TYPE, d);
        }

        public DateObFunction(Expression[] params) {
            super("dateob", params);

            if (parameterCount() > 1) {
                throw new RuntimeException(
                        "'dateob' function must have only one or zero parameters.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {

            // No parameters so return the current date.
            if (parameterCount() == 0) {
                return dateVal(new Date());
            }

            TObject exp_res = getParameter(0).evaluate(group, resolver, context);
            // If expression resolves to 'null' then return current date
            if (exp_res.isNull()) {
                return dateVal(new Date());
            }
            // If expression resolves to a BigDecimal, then treat as number of
            // seconds since midnight Jan 1st, 1970
            else if (exp_res.getTType() instanceof TNumericType) {
                BigNumber num = (BigNumber) exp_res.getObject();
                return dateVal(new Date(num.longValue()));
            }

            String date_str = exp_res.getObject().toString();

            // We need to synchronize here unfortunately because the Java
            // DateFormat objects are not thread-safe.
            synchronized (date_format_sho) {
                // Try and parse date
                try {
                    return dateVal(date_format_sql.parse(date_str));
                } catch (ParseException e) {
                }
                try {
                    return dateVal(date_format_sho.parse(date_str));
                } catch (ParseException e) {
                }
                try {
                    return dateVal(date_format_med.parse(date_str));
                } catch (ParseException e) {
                }
                try {
                    return dateVal(date_format_lon.parse(date_str));
                } catch (ParseException e) {
                }
                try {
                    return dateVal(date_format_ful.parse(date_str));
                } catch (ParseException e) {
                }

                throw new RuntimeException(
                        "Unable to parse date string '" + date_str + "'");
            }

        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return DATE_TYPE;
        }

    }

    static class TimeObFunction extends AbstractFunction {

        private final static TType TIME_TYPE = new TDateType(SQLTypes.TIME);

        public TimeObFunction(Expression[] params) {
            super("timeob", params);

            if (parameterCount() > 1) {
                throw new RuntimeException(
                        "'timeob' function must have only one or zero parameters.");
            }
        }

        private TObject timeNow() {
            Calendar c = Calendar.getInstance();
            c.setLenient(false);
            int hour = c.get(Calendar.HOUR_OF_DAY);
            int minute = c.get(Calendar.MINUTE);
            int second = c.get(Calendar.SECOND);
            int millisecond = c.get(Calendar.MILLISECOND);

            c.set(1970, 0, 1);
            return new TObject(TIME_TYPE, c.getTime());
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {

            // No parameters so return the current time.
            if (parameterCount() == 0) {
                return timeNow();
            }

            TObject exp_res = getParameter(0).evaluate(group, resolver, context);
            // If expression resolves to 'null' then return current date
            if (exp_res.isNull()) {
                return timeNow();
            }

            String date_str = exp_res.getObject().toString();

            return new TObject(TIME_TYPE, CastHelper.toTime(date_str));

        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TIME_TYPE;
        }

    }

    static class TimeStampObFunction extends AbstractFunction {

        private final static TType TIMESTAMP_TYPE =
                new TDateType(SQLTypes.TIMESTAMP);

        public TimeStampObFunction(Expression[] params) {
            super("timestampob", params);

            if (parameterCount() > 1) {
                throw new RuntimeException(
                        "'timestampob' function must have only one or zero parameters.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {

            // No parameters so return the current time.
            if (parameterCount() == 0) {
                return new TObject(TIMESTAMP_TYPE, new Date());
            }

            TObject exp_res = getParameter(0).evaluate(group, resolver, context);
            // If expression resolves to 'null' then return current date
            if (exp_res.isNull()) {
                return new TObject(TIMESTAMP_TYPE, new Date());
            }

            String date_str = exp_res.getObject().toString();

            return new TObject(TIMESTAMP_TYPE, CastHelper.toTimeStamp(date_str));

        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TIMESTAMP_TYPE;
        }

    }

    // A function that formats an input java.sql.Date object to the format
    // given using the java.text.SimpleDateFormat class.

    static class DateFormatFunction extends AbstractFunction {

        final static Cache formatter_cache = new Cache(127, 90, 10);

        public DateFormatFunction(Expression[] params) {
            super("dateformat", params);

            if (parameterCount() != 2) {
                throw new RuntimeException(
                        "'dateformat' function must have exactly two parameters.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {

            TObject datein = getParameter(0).evaluate(group, resolver, context);
            TObject format = getParameter(1).evaluate(group, resolver, context);
            // If expression resolves to 'null' then return null
            if (datein.isNull()) {
                return datein;
            }

            Date d;
            if (!(datein.getTType() instanceof TDateType)) {
                throw new RuntimeException(
                        "Date to format must be DATE, TIME or TIMESTAMP");
            } else {
                d = (Date) datein.getObject();
            }

            String format_string = format.toString();
            synchronized (formatter_cache) {
                SimpleDateFormat formatter =
                        (SimpleDateFormat) formatter_cache.get(format_string);
                if (formatter == null) {
                    formatter = new SimpleDateFormat(format_string);
                    formatter_cache.put(format_string, formatter);
                }
                return TObject.stringVal(formatter.format(d));
            }
        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return TType.STRING_TYPE;
        }

    }



}
