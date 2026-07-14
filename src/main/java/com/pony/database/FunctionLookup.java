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
 * An interface that resolves and generates a Function objects given a
 * FunctionDef object.
 *
 * @author Tobias Downer
 */

public interface FunctionLookup {

    /**
     * Generate the Function given a FunctionDef object.  Returns null if the
     * FunctionDef can not be resolved to a valid function object.  If the
     * specification of the function is invalid for some reason (the number or
     * type of the parameters is incorrect) then a StatementException is thrown.
     */
    Function generateFunction(FunctionDef function_def);

    /**
     * Returns true if the function defined by FunctionDef is an aggregate
     * function, or false otherwise.
     */
    boolean isAggregate(FunctionDef function_def);

}
