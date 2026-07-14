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

package com.pony.debug;

/**
 * Debug level static values.
 *
 * @author Tobias Downer
 */

public interface Lvl {

    /**
     * Some sample debug levels.
     */
    int INFORMATION = 10;    // General processing 'noise'
    int WARNING = 20;    // A message of some importance
    int ALERT = 30;    // Crackers, etc
    int ERROR = 40;    // Errors, exceptions
    int MESSAGE = 10000; // Always printed messages
    // (not error's however)

}
