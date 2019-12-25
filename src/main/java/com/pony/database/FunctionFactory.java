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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Set;

/**
 * A factory that generates Function objects given a function name and a
 * set of expression's that represent parameters.  A developer may create
 * their own instance of this class and register the factory with the
 * DatabaseSystem.  When the SQL grammer comes across a function, it will
 * try and resolve the function name against the registered function
 * factories.
 *
 * @author Tobias Downer
 */

public abstract class FunctionFactory implements FunctionLookup {

    private static final Expression GLOB_EXPRESSION;

    static {
        GLOB_EXPRESSION = new Expression();
        GLOB_EXPRESSION.addElement(TObject.stringVal("*"));
        GLOB_EXPRESSION.text().append("*");
    }

    /**
     * Represents a function argument * for glob's such as 'count(*)'
     */
    public static final Expression[] GLOB_LIST =
            new Expression[]{GLOB_EXPRESSION};

    /**
     * The mapping of 'fun_name' to 'fun_class' for each function that's
     * registered with this factory.
     */
    private HashMap fun_class_mapping;

    /**
     * Constructor arguments for the function.
     */
    private Class[] construct_proto;


    /**
     * Constructs the FunctionFactory.
     */
    public FunctionFactory() {
        fun_class_mapping = new HashMap();
        // The is the prototype for the constructor when creating a new function.
        construct_proto = new Class[1];
        Object exp_arr_ob =
                java.lang.reflect.Array.newInstance(new Expression().getClass(), 0);
        construct_proto[0] = exp_arr_ob.getClass();
    }

    /**
     * Adds a new function to this factory.  Takes a function name and a
     * class that is the Function implementation.  When the 'generateFunction'
     * method is called, it looks up the class with the function name and
     * returns a new instance of the function.
     * <p>
     * @param fun_name the name of the function (eg. 'sum', 'concat').
     * @param fun_class the Function class that we instantiate for this function.
     * @param fun_type that type of function (either FunctionInfo.STATIC,
     *   FunctionInfo.AGGREGATE, FunctionInfo.STATE_BASED).
     */
    protected void addFunction(String fun_name, Class fun_class, int fun_type) {
        try {
            String lf_name = fun_name.toLowerCase();
            if (fun_class_mapping.get(lf_name) == null) {
                FF_FunctionInfo ff_info = new FF_FunctionInfo(fun_name, fun_type,
                        fun_class.getConstructor(construct_proto));
                fun_class_mapping.put(lf_name, ff_info);
            } else {
                throw new Error("Function '" + fun_name +
                        "' already defined in factory.");
            }
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Adds a new static function to this factory.
     */
    protected void addFunction(String fun_name, Class fun_class) {
        addFunction(fun_name, fun_class, FunctionInfo.STATIC);
    }

    /**
     * Removes a static function from this factory.
     */
    protected void removeFunction(String fun_name) {
        String lf_name = fun_name.toLowerCase();
        if (fun_class_mapping.get(lf_name) != null) {
            fun_class_mapping.remove(fun_name.toLowerCase());
        } else {
            throw new Error("Function '" + lf_name +
                    "' is not defined in this factory.");
        }
    }

    /**
     * Returns true if the function name is defined in this factory.
     */
    protected boolean functionDefined(String fun_name) {
        String lf_name = fun_name.toLowerCase();
        return fun_class_mapping.get(lf_name) != null;
    }

    /**
     * Initializes this FunctionFactory.  This is an abstract method that
     * needs to be implemented.  (It doesn't need to do anything if a developer
     * implements their own version of 'generateFunction').
     */
    public abstract void init();

    /**
     * Creates a Function object for the function with the given name with the
     * given arguments.  If this factory does not handle a function with the
     * given name then it returns null.
     */
    public Function generateFunction(FunctionDef function_def) {
        //String func_name, Expression[] params) {

        String func_name = function_def.getName();
        Expression[] params = function_def.getParameters();

        // This will lookup the function name (case insensitive) and if a
        // function class was registered, instantiates and returns it.

        FF_FunctionInfo ff_info = (FF_FunctionInfo)
                fun_class_mapping.get(func_name.toLowerCase());
        if (ff_info == null) {
            // Function not handled by this factory so return null.
            return null;
        } else {
            Constructor fun_constructor = (Constructor) ff_info.getConstructor();
            Object[] args = new Object[]{params};
            try {
                return (Function) fun_constructor.newInstance(args);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e.getTargetException().getMessage());
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    /**
     * Returns true if the function defined by FunctionDef is an aggregate
     * function, or false otherwise.
     */
    public boolean isAggregate(FunctionDef function_def) {
        FunctionInfo f_info = getFunctionInfo(function_def.getName());
        if (f_info == null) {
            // Function not handled by this factory so return false.
            return false;
        } else {
            return (f_info.getType() == FunctionInfo.AGGREGATE);
        }
    }

    /**
     * Returns a FunctionInfo instance of the function with the given name that
     * this FunctionFactory manages.  If 'generateFunction' is reimplemented then
     * this method should be rewritten also.
     */
    public FunctionInfo getFunctionInfo(String fun_name) {
        FF_FunctionInfo ff_info = (FF_FunctionInfo)
                fun_class_mapping.get(fun_name.toLowerCase());
        return ff_info;
    }

    /**
     * Returns the list of all function names that this FunctionFactory manages.
     * This is used to compile information about the function factories.  If
     * 'generateFunction' is reimplemented then this method should be rewritten
     * also.
     */
    public FunctionInfo[] getAllFunctionInfo() {
        Set keys = fun_class_mapping.keySet();
        int list_size = keys.size();
        FunctionInfo[] list = new FunctionInfo[list_size];
        Iterator i = keys.iterator();
        int n = 0;
        while (i.hasNext()) {
            String fun_name = (String) i.next();
            list[n] = getFunctionInfo(fun_name);
            ++n;
        }
        return list;
    }

    /**
     * An implementation of FunctionInfo.
     */
    protected class FF_FunctionInfo implements FunctionInfo {

        private final String name;
        private final int type;
        private final Constructor constructor;

        public FF_FunctionInfo(String name, int type, Constructor constructor) {
            this.name = name;
            this.type = type;
            this.constructor = constructor;
        }

        public String getName() {
            return name;
        }

        public int getType() {
            return type;
        }

        public Constructor getConstructor() {
            return constructor;
        }

        public String getFunctionFactoryName() {
            return FunctionFactory.this.getClass().toString();
        }

    }

}
