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

package com.pony.database.jdbc;

/**
 * An interface that is input to the DatabaseInterface as a way to be
 * notified of event information from inside the database.
 *
 * @author Tobias Downer
 */

public interface DatabaseCallBack {

    /**
     * Called when the database has generated an event that this user is
     * listening for.
     * <p>
     * NOTE: The thread that calls back these events is always a volatile
     *   thread that may not block.  It is especially important that no queries
     *   are executed when this calls back.  To safely act on events, it is
     *   advisable to dispatch onto another thread such as the
     *   SwingEventDispatcher thread.
     */
    void databaseEvent(int event_type, String event_message);

}
