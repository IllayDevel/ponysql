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
 * An interface to resolve a variable name to a constant object.  This is used
 * as a way to resolve a variable into a value to use in an expression.
 *
 * @author Tobias Downer
 */

public interface VariableResolver {

    /**
     * A number that uniquely identifies the current state of the variable
     * resolver.  This typically returns the row_index of the table we are
     * resolving variables on.
     */
    int setID();

    /**
     * Returns the value of a given variable.
     */
    TObject resolve(Variable variable);

    /**
     * Returns the TType of object the given variable is.
     */
    TType returnTType(Variable variable);

}
