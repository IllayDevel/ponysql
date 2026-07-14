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
 * An interface that is notified of add/remove events on a
 * MasterTableDataSource.  The purpose of this interface is so that a high level
 * function can listen for changes to the underlying table and cache high
 * level representations of the rows as appropriate.
 *
 * @author Tobias Downer
 */

interface MasterTableListener {

    /**
     * Notifies of a new row addition to the underlying representation.  Note
     * that this addition doesn't necessarily mean that the change is a committed
     * change.  There is no way to tell if a change is committed or not.
     * <p>
     * SYNCHRONIZATION ISSUE: Note that extreme care should be taken with
     * deadlock issues with this method.  This is a call-back from
     * MasterTableDataSource when its monikor is in a synchronized state.  This
     * means there is potential for deadlock if care is not taken.  Listeners of
     * this should event should not try and inspect the state of the database.
     */
    void rowAdded(int row_number, RowData row_data);

    /**
     * Notifies that a row has been permanently removed from the underlying
     * representation.  This means the row has been committed removed and the
     * table row garbage collector has decided it is eligible to be recycled.
     * <p>
     * Normally the garbage collector thread will notify of this event.
     * <p>
     * SYNCHRONIZATION ISSUE: Note that extreme care should be taken with
     * deadlock issues with this method.  This is a call-back from
     * MasterTableDataSource when its monikor is in a synchronized state.  This
     * means there is potential for deadlock if care is not taken.  Listeners of
     * this should event should not try and inspect the state of the database.
     */
    void rowRemoved(int row_number);

}

