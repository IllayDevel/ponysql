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

package com.pony.tools;

import java.io.*;

import com.pony.database.*;
import com.pony.database.control.*;
import com.pony.util.CommandLine;

/**
 * A tool for converting between different versions of the database file
 * system.
 *
 * @author Tobias Downer
 */

public class DataFileConvertTool {

    /**
     * Prints the syntax.
     */
    private static void printSyntax() {
        System.out.println("DataFileConvertTool -path [data files path] " +
                "-u [admin username] -p [admin password]");
    }

    /**
     * Application start point.
     */
    public static void main(String[] args) {
        CommandLine cl = new CommandLine(args);

        String path = cl.switchArgument("-path");
        String admin_username = cl.switchArgument("-u");
        String admin_password = cl.switchArgument("-p");

        if (path == null) {
            printSyntax();
            System.out.println("Error: -path not found on command line.");
            System.exit(-1);
        }
        if (admin_username == null) {
            printSyntax();
            System.out.println("Error: -u [username] not found on command line.");
            System.exit(-1);
        }
        if (admin_password == null) {
            printSyntax();
            System.out.println("Error: -p [password] not found on command line.");
            System.exit(-1);
        }

        DatabaseSystem system = new DatabaseSystem();

        // Create a default configuration
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(path);
        config.setMinimumDebugLevel(Integer.MAX_VALUE);

        // Set up the log file
        system.setDebugLevel(Integer.MAX_VALUE);

        // Initialize the DatabaseSystem,
        // -----------------------------

        // This will throw an Error exception if the database system has already
        // been initialized.
        system.init(config);

        // Start the database class
        // ------------------------

        // Note, currently we only register one database, and it is named
        //   'DefaultDatabase'.
        Database database = new Database(system, "DefaultDatabase");

        boolean success = false;
        try {
            // Convert to the current version.
            success = database.convertToCurrent(System.out, admin_username);
        } catch (IOException e) {
            System.out.println("IO Error: " + e.getMessage());
            e.printStackTrace(System.out);
        }

        if (success) {
            System.out.println("-- Convert Successful --");
        } else {
            System.out.println("-- Convert Failed --");
        }

        // Shut down (and clean up) the database
        database.startShutDownThread();
        database.waitUntilShutdown();

    }

}
