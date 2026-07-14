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
 * A definition of a schema.
 *
 * @author Tobias Downer
 */

public final class SchemaDef {

    /**
     * The name of the schema (eg. APP).
     */
    private final String name;

    /**
     * The type of this schema (eg. SYSTEM, USER, etc)
     */
    private final String type;

    /**
     * Constructs the SchemaDef.
     */
    public SchemaDef(String name, String type) {
        this.name = name;
        this.type = type;
    }

    /**
     * Returns the case correct name of the schema.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the type of this schema.
     */
    public String getType() {
        return type;
    }

    public String toString() {
        return getName();
    }

}
