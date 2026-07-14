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

import java.util.ArrayList;

/**
 * A class that manages the list of users connected to the engine.
 * <p>
 * This class is thread safe, however it is recommended that the callee should
 * synchronize over this object when inspecting a subset of the user list.
 * The reason being that a user can connect or disconnect at any time.
 *
 * @author Tobias Downer
 */

public final class UserManager {

    /**
     * The list of User objects that are currently connected to the database
     * engine.
     */
    private final ArrayList user_list;

    /**
     * Constructs the UserManager.
     */
    UserManager() {
        user_list = new ArrayList();
    }

    /**
     * Called when a new user connects to the engine.
     */
    synchronized void userLoggedIn(User user) {
        if (!user_list.contains(user)) {
            user_list.add(user);
        } else {
            throw new Error("UserManager already has this User instance logged in.");
        }
    }

    /**
     * Called when the user logs out of the engine.
     */
    synchronized void userLoggedOut(User user) {
        user_list.remove(user);
    }

    /**
     * Returns the number of users that are logged in.
     */
    public synchronized int userCount() {
        return user_list.size();
    }

    /**
     * Returns the User object at index 'n' in the manager where 0 is the first
     * user.
     */
    public synchronized User userAt(int n) {
        return (User) user_list.get(n);
    }

}
