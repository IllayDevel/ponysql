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

/**
 * The name of a procedure as understood by a ProcedureManager.
 */

public class ProcedureName {

    /**
     * The schema of this procedure.
     */
    private final String schema;

    /**
     * The name of this procedure.
     */
    private final String name;

    /**
     * Constructs the ProcedureName.
     */
    public ProcedureName(String schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    /**
     * Constructs the ProcedureName from a TableName.
     */
    public ProcedureName(TableName table_name) {
        this(table_name.getSchema(), table_name.getName());
    }

    /**
     * Returns the schema of this procedure.
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Returns the name of this procedure.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns this procedure name as a string.
     */
    public String toString() {
        return schema + "." + name;
    }

    /**
     * Returns a version of this procedure qualified to the given schema (unless
     * the schema is present).
     */
    public static ProcedureName qualify(String current_schema, String proc_name) {
        int delim = proc_name.indexOf(".");
        if (delim == -1) {
            return new ProcedureName(current_schema, proc_name);
        } else {
            return new ProcedureName(proc_name.substring(0, delim),
                    proc_name.substring(delim + 1));
        }
    }

    /**
     * Equality test.
     */
    public boolean equals(Object ob) {
        ProcedureName src_ob = (ProcedureName) ob;
        return (schema.equals(src_ob.schema) &&
                name.equals(src_ob.name));
    }

    /**
     * The hash key.
     */
    public int hashCode() {
        return schema.hashCode() + name.hashCode();
    }

}

