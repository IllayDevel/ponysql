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

import java.lang.reflect.*;

import com.pony.database.global.BlobAccessor;
import com.pony.database.global.StringAccessor;
import com.pony.util.BigNumber;

import java.io.IOException;
import java.io.InputStream;
import java.util.StringTokenizer;
import java.util.ArrayList;

/**
 * A DatabaseConnection procedure manager.  This controls adding, updating,
 * deleting and querying/calling stored procedures.
 *
 * @author Tobias Downer
 */

public class ProcedureManager {

    /**
     * The DatabaseConnection.
     */
    private DatabaseConnection connection;

    /**
     * The context.
     */
    private DatabaseQueryContext context;

    /**
     * Constructs the ProcedureManager for a DatabaseConnection.
     */
    ProcedureManager(DatabaseConnection connection) {
        this.connection = connection;
        this.context = new DatabaseQueryContext(connection);
    }

    /**
     * Given the SYS_FUNCTION table, this returns a new table that contains the
     * entry with the given procedure name, or an empty result if nothing found.
     * Generates an error if more than 1 entry found.
     */
    private Table findProcedureEntry(DataTable table,
                                     ProcedureName procedure_name) {

        Operator EQUALS = Operator.get("=");

        Variable schemav = table.getResolvedVariable(0);
        Variable namev = table.getResolvedVariable(1);

        Table t = table.simpleSelect(context, namev, EQUALS,
                new Expression(TObject.stringVal(procedure_name.getName())));
        t = t.exhaustiveSelect(context, Expression.simple(
                schemav, EQUALS, TObject.stringVal(procedure_name.getSchema())));

        // This should be at most 1 row in size
        if (t.getRowCount() > 1) {
            throw new RuntimeException(
                    "Assert failed: multiple procedure names for " + procedure_name);
        }

        // Return the entries found.
        return t;

    }

    /**
     * Formats a string that gives information about the procedure, return
     * type and param types.
     */
    private static String procedureInfoString(ProcedureName name,
                                              TType ret, TType[] params) {
        StringBuffer buf = new StringBuffer();
        if (ret != null) {
            buf.append(ret.asSQLString());
            buf.append(" ");
        }
        buf.append(name.getName());
        buf.append("(");
        for (int i = 0; i < params.length; ++i) {
            buf.append(params[i].asSQLString());
            if (i < params.length - 1) {
                buf.append(", ");
            }
        }
        buf.append(")");
        return new String(buf);
    }

    /**
     * Given a location string as defined for a Java stored procedure, this
     * parses the string into the various parts.  For example, given the
     * string 'com.mycompany.storedprocedures.MyFunctions.minFunction()' this
     * will parse the string out to the class called
     * 'com.mycompany.storedprocedures.MyFunctions' and the method 'minFunction'
     * with no arguments.  This function will work event if the method name is
     * not given, or the method name does not have an arguments specification.
     */
    public static String[] parseJavaLocationString(final String str) {
        // Look for the first parenthese
        int parenthese_delim = str.indexOf("(");
        String class_method;

        if (parenthese_delim != -1) {
            // This represents class/method
            class_method = str.substring(0, parenthese_delim);
            // This will be deliminated by a '.'
            int method_delim = class_method.lastIndexOf(".");
            if (method_delim == -1) {
                throw new StatementException(
                        "Incorrectly formatted Java method string: " + str);
            }
            String class_str = class_method.substring(0, method_delim);
            String method_str = class_method.substring(method_delim + 1);
            // Next parse the argument list
            int end_parenthese_delim = str.lastIndexOf(")");
            if (end_parenthese_delim == -1) {
                throw new StatementException(
                        "Incorrectly formatted Java method string: " + str);
            }
            String arg_list_str =
                    str.substring(parenthese_delim + 1, end_parenthese_delim);
            // Now parse the list of arguments
            ArrayList arg_list = new ArrayList();
            StringTokenizer tok = new StringTokenizer(arg_list_str, ",");
            while (tok.hasMoreTokens()) {
                String arg = tok.nextToken();
                arg_list.add(arg);
            }

            // Form the parsed array and return it
            int sz = arg_list.size();
            String[] return_array = new String[2 + sz];
            return_array[0] = class_str;
            return_array[1] = method_str;
            for (int i = 0; i < sz; ++i) {
                return_array[i + 2] = (String) arg_list.get(i);
            }
            return return_array;

        } else {
            // No parenthese so we assume this is a java class
            return new String[]{str};
        }

    }

    /**
     * Returns true if the procedure with the given name exists.
     */
    public boolean procedureExists(ProcedureName procedure_name) {

        DataTable table = connection.getTable(Database.SYS_FUNCTION);
        return findProcedureEntry(table, procedure_name).getRowCount() == 1;

    }

    /**
     * Returns true if the procedure with the given table name exists.
     */
    public boolean procedureExists(TableName procedure_name) {
        return procedureExists(new ProcedureName(procedure_name));
    }

    /**
     * Defines a Java stored procedure.  If the procedure with the name has not
     * been defined it is defined.  If the procedure has been defined then it is
     * overwritten with this information.
     * <p>
     * If 'return_type' is null then the procedure does not return a value.
     */
    public void defineJavaProcedure(ProcedureName procedure_name,
                                    String java_specification,
                                    TType return_type, TType[] param_types,
                                    String username)
            throws DatabaseException {

        TableName proc_table_name =
                new TableName(procedure_name.getSchema(), procedure_name.getName());

        // Check this name is not reserved
        DatabaseConnection.checkAllowCreate(proc_table_name);

        DataTable table = connection.getTable(Database.SYS_FUNCTION);

        // The new row to insert/update
        RowData row_data = new RowData(table);
        row_data.setColumnDataFromObject(0, procedure_name.getSchema());
        row_data.setColumnDataFromObject(1, procedure_name.getName());
        row_data.setColumnDataFromObject(2, "Java-1");
        row_data.setColumnDataFromObject(3, java_specification);
        if (return_type != null) {
            row_data.setColumnDataFromObject(4, TType.asEncodedString(return_type));
        }
        row_data.setColumnDataFromObject(5, TType.asEncodedString(param_types));
        row_data.setColumnDataFromObject(6, username);

        // Find the entry from the procedure table that equal this name
        Table t = findProcedureEntry(table, procedure_name);

        // Delete the entry if it already exists.
        if (t.getRowCount() == 1) {
            table.delete(t);
        }

        // Insert the new entry,
        table.add(row_data);

        // Notify that this database object has been successfully created.
        connection.databaseObjectCreated(proc_table_name);

    }

    /**
     * Deletes the procedure with the given name, or generates an error if the
     * procedure doesn't exist.
     */
    public void deleteProcedure(ProcedureName procedure_name)
            throws DatabaseException {

        DataTable table = connection.getTable(Database.SYS_FUNCTION);

        // Find the entry from the procedure table that equal this name
        Table t = findProcedureEntry(table, procedure_name);

        // If no entries then generate error.
        if (t.getRowCount() == 0) {
            throw new StatementException("Procedure " + procedure_name +
                    " doesn't exist.");
        }

        table.delete(t);

        // Notify that this database object has been successfully dropped.
        connection.databaseObjectDropped(
                new TableName(procedure_name.getSchema(), procedure_name.getName()));

    }

    /**
     * Returns an InternalTableInfo object used to model the list of procedures
     * that are accessible within the given Transaction object.  This is used to
     * model all procedures that have been defined as tables.
     */
    static InternalTableInfo createInternalTableInfo(Transaction transaction) {
        return new ProcedureInternalTableInfo(transaction);
    }

    /**
     * Invokes the procedure with the given name and the given parameters and
     * returns the procedure return value.
     */
    public TObject invokeProcedure(ProcedureName procedure_name,
                                   TObject[] params) {

        DataTable table = connection.getTable(Database.SYS_FUNCTION);

        // Find the entry from the procedure table that equals this name
        Table t = findProcedureEntry(table, procedure_name);
        if (t.getRowCount() == 0) {
            throw new StatementException("Procedure " + procedure_name +
                    " doesn't exist.");
        }

        int row_index = t.rowEnumeration().nextRowIndex();
        TObject type_ob = t.getCellContents(2, row_index);
        TObject location_ob = t.getCellContents(3, row_index);
        TObject return_type_ob = t.getCellContents(4, row_index);
        TObject param_types_ob = t.getCellContents(5, row_index);
        TObject owner_ob = t.getCellContents(6, row_index);

        String type = type_ob.getObject().toString();
        String location = location_ob.getObject().toString();
        TType return_type = null;
        if (!return_type_ob.isNull()) {
            return_type = TType.decodeString(return_type_ob.getObject().toString());
        }
        TType[] param_types =
                TType.decodeTypes(param_types_ob.getObject().toString());
        String owner = owner_ob.getObject().toString();

        // Check the number of parameters given match the function parameters length
        if (params.length != param_types.length) {
            throw new StatementException(
                    "Parameters given do not match the parameters of the procedure: " +
                            procedureInfoString(procedure_name, return_type, param_types));
        }

        // The different procedure types,
        if (type.equals("Java-1")) {
            return invokeJavaV1Procedure(procedure_name, location,
                    return_type, param_types, owner, params);
        } else {
            throw new RuntimeException("Unknown procedure type: " + type);
        }

    }

    /**
     * Resolves a Java class specification string to a Java Class object.  For
     * example, "String" becomes 'java.lang.String.class' and "boolean[]" becomes
     * 'boolean[].class', etc.
     */
    private static Class resolveToClass(String java_spec) {
        // Trim the string
        java_spec = java_spec.trim();
        // Is this an array?  Count the number of array dimensions.
        int dimensions = -1;
        int last_index = java_spec.length();
        while (last_index > 0) {
            ++dimensions;
            last_index = java_spec.lastIndexOf("[]", last_index) - 1;
        }
        // Remove the array part
        int array_end = java_spec.length() - (dimensions * 2);
        String class_part = java_spec.substring(0, array_end);
        // Check there's no array parts in the class part
        if (class_part.indexOf("[]") != -1) {
            throw new RuntimeException(
                    "Java class specification incorrectly formatted: " + java_spec);
        }

        // Convert the java specification to a Java class.  For example,
        // String is converted to java.lang.String.class, etc.
        Class cl;
        // Is there a '.' in the class specification?
        if (class_part.indexOf(".") != -1) {
            // Must be a specification such as 'java.net.URL' or 'java.util.List'.
            try {
                cl = Class.forName(class_part);
            } catch (ClassNotFoundException i) {
                throw new RuntimeException("Java class not found: " + class_part);
            }
        }

        // Try for a primitive types
        else if (class_part.equals("boolean")) {
            cl = boolean.class;
        } else if (class_part.equals("byte")) {
            cl = byte.class;
        } else if (class_part.equals("short")) {
            cl = short.class;
        } else if (class_part.equals("char")) {
            cl = char.class;
        } else if (class_part.equals("int")) {
            cl = int.class;
        } else if (class_part.equals("long")) {
            cl = long.class;
        } else if (class_part.equals("float")) {
            cl = float.class;
        } else if (class_part.equals("double")) {
            cl = double.class;
        } else {
            // Not a primitive type so try resolving against java.lang.* or some
            // key classes in com.pony.database.*
            if (class_part.equals("ProcedureConnection")) {
                cl = ProcedureConnection.class;
            } else {
                try {
                    cl = Class.forName("java.lang." + class_part);
                } catch (ClassNotFoundException i) {
                    // No luck so give up,
                    throw new RuntimeException("Java class not found: " + class_part);
                }
            }
        }

        // Finally make into a dimension if necessary
        if (dimensions > 0) {
            // This is a little untidy way of doing this.  Perhaps a better approach
            // would be to make an array encoded string (eg. "[[Ljava.langString;").
            cl = java.lang.reflect.Array.newInstance(cl,
                    new int[dimensions]).getClass();
        }

        return cl;

    }


    /**
     * Given a Java location_str and a list of parameter types, returns an
     * immutable 'Method' object that can be used to invoke a Java stored
     * procedure.  The returned object can be cached if necessary.  Note that
     * this method will generate an error for the following situations:
     * a) The invokation class or method was not found, b) there is not an
     * invokation method with the required number of arguments or that matches
     * the method specification.
     * <p>
     * Returns null if the invokation method could not be found.
     */
    public static Method javaProcedureMethod(
            String location_str, TType[] param_types) {
        // Parse the location string
        String[] loc_parts = parseJavaLocationString(location_str);

        // The name of the class
        String class_name;
        // The name of the invokation method in the class.
        String method_name;
        // The object specification that must be matched.  If any entry is 'null'
        // then the argument parameter is discovered.
        Class[] object_specification;
        boolean firstProcedureConnectionIgnore;

        if (loc_parts.length == 1) {
            // This means the location_str only specifies a class name, so we use
            // 'invoke' as the static method to call, and discover the arguments.
            class_name = loc_parts[0];
            method_name = "invoke";
            // All null which means we discover the arg types dynamically
            object_specification = new Class[param_types.length];
            // ignore ProcedureConnection is first argument
            firstProcedureConnectionIgnore = true;
        } else {
            // This means we specify a class and method name and argument
            // specification.
            class_name = loc_parts[0];
            method_name = loc_parts[1];
            object_specification = new Class[loc_parts.length - 2];

            for (int i = 0; i < loc_parts.length - 2; ++i) {
                String java_spec = loc_parts[i + 2];
                object_specification[i] = resolveToClass(java_spec);
            }

            firstProcedureConnectionIgnore = false;
        }

        Class procedure_class;
        try {
            // Reference the procedure's class.
            procedure_class = Class.forName(class_name);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Procedure class not found: " +
                    class_name);
        }

        // Get all the methods in this class
        Method[] methods = procedure_class.getMethods();
        Method invoke_method = null;
        // Search for the invoke method
        for (int i = 0; i < methods.length; ++i) {
            Method method = methods[i];
            int modifier = method.getModifiers();

            if (Modifier.isStatic(modifier) && Modifier.isPublic(modifier) &&
                    method.getName().equals(method_name)) {

                boolean params_match;

                // Get the parameters for this method
                Class[] method_args = method.getParameterTypes();
                // If no methods, and object_specification has no args then this is a
                // match.
                if (method_args.length == 0 && object_specification.length == 0) {
                    params_match = true;
                } else {
                    int search_start = 0;
                    // Is the first arugments a ProcedureConnection implementation?
                    if (firstProcedureConnectionIgnore &&
                            ProcedureConnection.class.isAssignableFrom(method_args[0])) {
                        search_start = 1;
                    }
                    // Do the number of arguments match
                    if (object_specification.length ==
                            method_args.length - search_start) {
                        // Do they match the specification?
                        boolean match_spec = true;
                        for (int n = 0;
                             n < object_specification.length && match_spec == true;
                             ++n) {
                            Class ob_spec = object_specification[n];
                            if (ob_spec != null &&
                                    ob_spec != method_args[n + search_start]) {
                                match_spec = false;
                            }
                        }
                        params_match = match_spec;
                    } else {
                        params_match = false;
                    }
                }

                if (params_match) {
                    if (invoke_method == null) {
                        invoke_method = method;
                    } else {
                        throw new RuntimeException("Ambiguous public static " +
                                method_name + " methods in stored procedure class '" +
                                class_name + "'");
                    }
                }

            }

        }

        // Return the invoke method we found
        return invoke_method;

    }


    // ---------- Various procedure type invokation methods ----------

    /**
     * Invokes a Java (type 1) procedure.  A type 1 procedure is represented by
     * a single class with a static invokation method (called invoke).  The
     * parameters of the static 'invoke' method must be compatible class
     * parameters defined for the procedure, and the return class must also be
     * compatible with the procedure return type.
     * <p>
     * If the invoke method does not contain arguments that are compatible with
     * the parameters given an exception is generated.
     * <p>
     * The class must only have a single public static 'invoke' method.  If there
     * are multiple 'invoke' methods a runtime exception is generated.
     */
    private TObject invokeJavaV1Procedure(ProcedureName procedure_name,
                                          String location_str, TType return_type, TType[] param_types,
                                          String owner, TObject[] param_values) {

        // Search for the invokation method for this stored procedure
        Method invoke_method = javaProcedureMethod(location_str, param_types);

        // Did we find an invoke method?
        if (invoke_method == null) {
            throw new RuntimeException("Could not find the invokation method for " +
                    "the Java location string '" + location_str + "'");
        }

        // Go through each argument of this class and work out how we are going
        // cast from the database engine object to the Java object.
        Class[] java_param_types = invoke_method.getParameterTypes();

        // Is the first param a ProcedureConnection implementation?
        int start_param;
        Object[] java_values;
        if (java_param_types.length > 0 &&
                ProcedureConnection.class.isAssignableFrom(java_param_types[0])) {
            start_param = 1;
            java_values = new Object[param_types.length + 1];
        } else {
            start_param = 0;
            java_values = new Object[param_types.length];
        }

        // For each type
        for (int i = 0; i < param_types.length; ++i) {
            TObject value = param_values[i];
            TType proc_type = param_types[i];
            Class java_type = java_param_types[i + start_param];
            String java_type_str = java_type.getName();

            // First null check,
            if (value.isNull()) {
                java_values[i + start_param] = null;
            } else {
                TType value_type = value.getTType();
                // If not null, is the value and the procedure type compatible
                if (proc_type.comparableTypes(value_type)) {

                    boolean error_cast = false;
                    Object cast_value = null;

                    // Compatible types,
                    // Now we need to convert the parameter value into a Java object,
                    if (value_type instanceof TStringType) {
                        // A String type can be represented in Java as a java.lang.String,
                        // or as a java.io.Reader.
                        StringAccessor accessor = (StringAccessor) value.getObject();
                        if (java_type == java.lang.String.class) {
                            cast_value = accessor.toString();
                        } else if (java_type == java.io.Reader.class) {
                            cast_value = accessor.getReader();
                        } else {
                            error_cast = true;
                        }
                    } else if (value_type instanceof TBooleanType) {
                        // A boolean in Java is either java.lang.Boolean or primitive
                        // boolean.
                        if (java_type == java.lang.Boolean.class ||
                                java_type == boolean.class) {
                            cast_value = value.getObject();
                        } else {
                            error_cast = true;
                        }
                    } else if (value_type instanceof TDateType) {
                        // A date translates to either java.util.Date, java.sql.Date,
                        // java.sql.Timestamp, java.sql.Time.
                        java.util.Date d = (java.util.Date) value.getObject();
                        if (java_type == java.util.Date.class) {
                            cast_value = d;
                        } else if (java_type == java.sql.Date.class) {
                            cast_value = new java.sql.Date(d.getTime());
                        } else if (java_type == java.sql.Time.class) {
                            cast_value = new java.sql.Time(d.getTime());
                        } else if (java_type == java.sql.Timestamp.class) {
                            cast_value = new java.sql.Timestamp(d.getTime());
                        } else {
                            error_cast = true;
                        }
                    } else if (value_type instanceof TNumericType) {
                        // Number can be cast to any one of the Java numeric types
                        BigNumber num = (BigNumber) value.getObject();
                        if (java_type == BigNumber.class) {
                            cast_value = num;
                        } else if (java_type == java.lang.Byte.class ||
                                java_type == byte.class) {
                            cast_value = new Byte(num.byteValue());
                        } else if (java_type == java.lang.Short.class ||
                                java_type == short.class) {
                            cast_value = new Short(num.shortValue());
                        } else if (java_type == java.lang.Integer.class ||
                                java_type == int.class) {
                            cast_value = new Integer(num.intValue());
                        } else if (java_type == java.lang.Long.class ||
                                java_type == long.class) {
                            cast_value = new Long(num.longValue());
                        } else if (java_type == java.lang.Float.class ||
                                java_type == float.class) {
                            cast_value = new Float(num.floatValue());
                        } else if (java_type == java.lang.Double.class ||
                                java_type == double.class) {
                            cast_value = new Double(num.doubleValue());
                        } else if (java_type == java.math.BigDecimal.class) {
                            cast_value = num.asBigDecimal();
                        } else {
                            error_cast = true;
                        }
                    } else if (value_type instanceof TBinaryType) {
                        // A binary type can translate to a java.io.InputStream or a
                        // byte[] array.
                        BlobAccessor blob = (BlobAccessor) value.getObject();
                        if (java_type == java.io.InputStream.class) {
                            cast_value = blob.getInputStream();
                        } else if (java_type == byte[].class) {
                            byte[] buf = new byte[blob.length()];
                            try {
                                InputStream in = blob.getInputStream();
                                int n = 0;
                                int len = blob.length();
                                while (len > 0) {
                                    int count = in.read(buf, n, len);
                                    if (count == -1) {
                                        throw new IOException("End of stream.");
                                    }
                                    n += count;
                                    len -= count;
                                }
                            } catch (IOException e) {
                                throw new RuntimeException("IO Error: " + e.getMessage());
                            }
                            cast_value = buf;
                        } else {
                            error_cast = true;
                        }

                    }

                    // If the cast of the parameter was not possible, report the error.
                    if (error_cast) {
                        throw new StatementException("Unable to cast argument " + i +
                                " ... " + value_type.asSQLString() + " to " + java_type_str +
                                " for procedure: " +
                                procedureInfoString(procedure_name, return_type, param_types));
                    }

                    // Set the java value for this parameter
                    java_values[i + start_param] = cast_value;

                } else {
                    // The parameter is not compatible -
                    throw new StatementException("Parameter (" + i + ") not compatible " +
                            value.getTType().asSQLString() + " -> " + proc_type.asSQLString() +
                            " for procedure: " +
                            procedureInfoString(procedure_name, return_type, param_types));
                }

            }  // if not null

        }  // for each parameter

        // Create the user that has the privs of this procedure.
        User priv_user = new User(owner, connection.getDatabase(),
                "/Internal/Procedure/", System.currentTimeMillis());

        // Create the ProcedureConnection object.
        ProcedureConnection proc_connection =
                connection.createProcedureConnection(priv_user);
        Object result;
        try {
            // Now the 'connection' will be set to the owner's user privs.

            // Set the ProcedureConnection object as an argument if necessary.
            if (start_param > 0) {
                java_values[0] = proc_connection;
            }

            // The java_values array should now contain the parameter values formatted
            // as Java objects.

            // Invoke the method
            try {
                result = invoke_method.invoke(null, java_values);
            } catch (IllegalAccessException e) {
                connection.Debug().writeException(e);
                throw new StatementException("Illegal access exception when invoking " +
                        "stored procedure: " + e.getMessage());
            } catch (InvocationTargetException e) {
                Throwable real_e = e.getTargetException();
                connection.Debug().writeException(real_e);
                throw new StatementException("Procedure Exception: " +
                        real_e.getMessage());
            }

        } finally {
            connection.disposeProcedureConnection(proc_connection);
        }

        // If return_type is null, there is no result from this procedure (void)
        if (return_type == null) {
            return null;
        } else {
            // Cast to a valid return object and return.
            return TObject.createAndCastFromObject(return_type, result);
        }

    }

    // ---------- Inner classes ----------

    /**
     * An object that models the list of procedures as table objects in a
     * transaction.
     */
    private static class ProcedureInternalTableInfo
            extends AbstractInternalTableInfo2 {

        ProcedureInternalTableInfo(Transaction transaction) {
            super(transaction, Database.SYS_FUNCTION);
        }

        private static DataTableDef createDataTableDef(String schema, String name) {
            // Create the DataTableDef that describes this entry
            DataTableDef def = new DataTableDef();
            def.setTableName(new TableName(schema, name));

            // Add column definitions
            def.addColumn(DataTableColumnDef.createStringColumn("type"));
            def.addColumn(DataTableColumnDef.createStringColumn("location"));
            def.addColumn(DataTableColumnDef.createStringColumn("return_type"));
            def.addColumn(DataTableColumnDef.createStringColumn("param_args"));
            def.addColumn(DataTableColumnDef.createStringColumn("owner"));

            // Set to immutable
            def.setImmutable();

            // Return the data table def
            return def;
        }


        public String getTableType(int i) {
            return "FUNCTION";
        }

        public DataTableDef getDataTableDef(int i) {
            TableName table_name = getTableName(i);
            return createDataTableDef(table_name.getSchema(), table_name.getName());
        }

        public MutableTableDataSource createInternalTable(int index) {
            MutableTableDataSource table =
                    transaction.getTable(Database.SYS_FUNCTION);
            RowEnumeration row_e = table.rowEnumeration();
            int p = 0;
            int i;
            int row_i = -1;
            while (row_e.hasMoreRows()) {
                i = row_e.nextRowIndex();
                if (p == index) {
                    row_i = i;
                } else {
                    ++p;
                }
            }
            if (p == index) {
                String schema = table.getCellContents(0, row_i).getObject().toString();
                String name = table.getCellContents(1, row_i).getObject().toString();

                final DataTableDef table_def = createDataTableDef(schema, name);
                final TObject type = table.getCellContents(2, row_i);
                final TObject location = table.getCellContents(3, row_i);
                final TObject return_type = table.getCellContents(4, row_i);
                final TObject param_types = table.getCellContents(5, row_i);
                final TObject owner = table.getCellContents(6, row_i);

                // Implementation of MutableTableDataSource that describes this
                // procedure.
                return new GTDataSource(transaction.getSystem()) {
                    public DataTableDef getDataTableDef() {
                        return table_def;
                    }

                    public int getRowCount() {
                        return 1;
                    }

                    public TObject getCellContents(int col, int row) {
                        switch (col) {
                            case 0:
                                return type;
                            case 1:
                                return location;
                            case 2:
                                return return_type;
                            case 3:
                                return param_types;
                            case 4:
                                return owner;
                            default:
                                throw new RuntimeException("Column out of bounds.");
                        }
                    }
                };

            } else {
                throw new RuntimeException("Index out of bounds.");
            }

        }

    }

}

