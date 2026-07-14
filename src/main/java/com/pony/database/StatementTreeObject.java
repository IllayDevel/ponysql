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
 * An complex object that is to be contained within a StatementTree object.
 * A statement tree object must be serializable, and it must be able to
 * reference all Expression objects so that they may be prepared.
 *
 * @author Tobias Downer
 */

public interface StatementTreeObject {

    /**
     * Prepares all expressions in this statement tree object by passing the
     * ExpressionPreparer object to the 'prepare' method of the expression.
     */
    void prepareExpressions(ExpressionPreparer preparer)
            throws DatabaseException;

    /**
     * Performs a DEEP clone of this object if it is mutable, or a deep clone
     * of its mutable members.  If the object is immutable then it may return
     * 'this'.
     */
    Object clone() throws CloneNotSupportedException;

}
