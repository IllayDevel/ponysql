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
 * A listener that is notified when the trigger being listened to is fired.
 *
 * @author Tobias Downer
 */

public interface TriggerListener {

    /**
     * Notifies this listener that the trigger with the name has been fired.
     * Trigger's are specified via the SQL syntax and a trigger listener can
     * be registered via PonyConnection.
     *
     * @param trigger_name the name of the trigger that fired.
     */
    void triggerFired(String trigger_name);

}
