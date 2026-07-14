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

