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
 * An interface used to prepare an Expression object.  This interface is used
 * to mutate an element of an Expression from one form to another.  For
 * example, we may use this to translate a StatementTree object to a
 * Statement object.
 *
 * @author Tobias Downer
 */

public interface ExpressionPreparer {

    /**
     * Returns true if this preparer will prepare the given object in an
     * expression.
     */
    boolean canPrepare(Object element);

    /**
     * Returns the new translated object to be mutated from the given element.
     */
    Object prepare(Object element) throws DatabaseException;

}
