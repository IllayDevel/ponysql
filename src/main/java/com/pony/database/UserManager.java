/*
 * Pony SQL Database ( http://www.ponysql.ru/ )
 * Copyright (C) 2019-2020 IllayDevel.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    private ArrayList user_list;

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
