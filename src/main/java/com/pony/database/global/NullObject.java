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

package com.pony.database.global;

/**
 * A Null Object.
 *
 * @author Tobias Downer
 * @deprecated do not use.  Nulls are now handled via TObject and TType.  This
 *   method is only kept around for legacy with older databases.
 */

public class NullObject implements java.io.Serializable {

    static final long serialVersionUID = 8599490526855696529L;

    public static NullObject NULL_OBJ = new NullObject();

    public int compareTo(Object ob) {
        if (ob == null || ob instanceof NullObject) {
            return 0;
        }
        return -1;
    }

    public String toString() {
        return "NULL";
    }

}
