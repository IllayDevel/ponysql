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

package com.pony.database.control;

import com.pony.database.Database;
import com.pony.database.DatabaseSystem;
import com.pony.database.DatabaseException;
import com.pony.debug.*;
import com.pony.util.LogWriter;

import java.io.File;
import java.io.Writer;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.Date;

/**
 * An object that provides methods for creating and controlling database
 * systems in the current JVM.
 *
 * @author Tobias Downer
 */

public final class DBController {

    /**
     * This object can not be constructed outside of this package.
     */
    DBController() {
    }

    /**
     * Returns true if a Pony database exists in the given directory of the
     * file system, otherwise returns false if the path doesn't contain a
     * database.
     * <p>
     * The path string must be formatted using Unix '/' deliminators as
     * directory separators.
     *
     * @param config the configuration of the database to check the existence
     *   of.
     * @return true if a database exists at the given path, false otherwise.
     */
    public boolean databaseExists(DBConfig config) {
        Database database = createDatabase(config);
        boolean b = database.exists();
        database.getSystem().dispose();
        return b;
    }

    /**
     * Creates a database in the local JVM (and filesystem) given the
     * configuration in DBConfig and returns a DBSystem object.  When this
     * method returns, the database created will be up and running providing
     * there was no failure during the database creation process.
     * <p>
     * A failure might happen because the database path does not exist.
     *
     * @param admin_user the username of the administrator for the new database.
     * @param admin_pass the password of the administrator for the new database.
     * @param config the configuration of the database to create and start in the
     *   local JVM.
     * @return the DBSystem object used to access the database created.
     */
    public DBSystem createDatabase(DBConfig config,
                                   String admin_user, String admin_pass) {

        // Create the Database object with this configuration.
        Database database = createDatabase(config);
        DatabaseSystem system = database.getSystem();

        // Create the database.
        try {
            database.create(admin_user, admin_pass);
            database.init();
        } catch (DatabaseException e) {
            system.Debug().write(Lvl.ERROR, this, "Database create failed");
            system.Debug().writeException(e);
            throw new RuntimeException(e.getMessage());
        }

        // Return the DBSystem object for the newly created database.
        return new DBSystem(this, config, database);

    }

    /**
     * Starts a database in the local JVM given the configuration in DBConfig
     * and returns a DBSystem object.  When this method returns, the database
     * will be up and running providing there was no failure to initialize the
     * database.
     * <p>
     * A failure might happen if the database does not exist in the path given
     * in the configuration.
     *
     * @param config the configuration of the database to start in the local
     *   JVM.
     * @return the DBSystem object used to access the database started.
     */
    public DBSystem startDatabase(DBConfig config) {

        // Create the Database object with this configuration.
        Database database = createDatabase(config);
        DatabaseSystem system = database.getSystem();

        // First initialise the database
        try {
            database.init();
        } catch (DatabaseException e) {
            system.Debug().write(Lvl.ERROR, this, "Database init failed");
            system.Debug().writeException(e);
            throw new RuntimeException(e.getMessage());
        }

        // Return the DBSystem object for the newly created database.
        return new DBSystem(this, config, database);

    }


    // ---------- Static methods ----------

    /**
     * Creates a Database object for the given DBConfig configuration.
     */
    private static Database createDatabase(DBConfig config) {

        DatabaseSystem system = new DatabaseSystem();

        // Initialize the DatabaseSystem first,
        // ------------------------------------

        // This will throw an Error exception if the database system has already
        // been initialized.
        system.init(config);

        // Start the database class
        // ------------------------

        // Note, currently we only register one database, and it is named
        //   'DefaultDatabase'.
        Database database = new Database(system, "DefaultDatabase");

        // Start up message
        system.Debug().write(Lvl.MESSAGE, DBController.class,
                "Starting Database Server");

        return database;
    }

    /**
     * Returns the static controller for this JVM.
     */
    public static DBController getDefault() {
        return VM_DB_CONTROLLER;
    }

    /**
     * The static DBController object.
     */
    private final static DBController VM_DB_CONTROLLER = new DBController();

}
