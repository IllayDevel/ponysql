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

import com.pony.database.*;
import com.pony.util.CommandLine;
import com.pony.util.ShellUserTerminal;
import com.pony.database.control.*;

import java.io.*;

/**
 * A command line repair tool for repairing a corrupted conglomerate.
 *
 * @author Tobias Downer
 */

public class DBConglomerateRepairTool {

    private static void repair(String path, String name) {

        ShellUserTerminal terminal = new ShellUserTerminal();

        TransactionSystem system = new TransactionSystem();
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(path);
        config.setLogPath("");
        config.setMinimumDebugLevel(50000);
        // We do not use the NIO API for repairs for safety.
        config.setValue("do_not_use_nio_api", "enabled");
        system.setDebugOutput(new StringWriter());
        system.init(config);
        final TableDataConglomerate conglomerate =
                new TableDataConglomerate(system, system.storeSystem());
        // Check it.
        conglomerate.fix(name, terminal);

        // Dispose the transaction system
        system.dispose();
    }

    /**
     * Prints the syntax.
     */
    private static void printSyntax() {
        System.out.println("DBConglomerateRepairTool -path [data directory] " +
                "[-name [database name]]");
    }

    /**
     * Application start point.
     */
    public static void main(String[] args) {
        CommandLine cl = new CommandLine(args);

        String path = cl.switchArgument("-path");
        String name = cl.switchArgument("-name", "DefaultDatabase");

        if (path == null) {
            printSyntax();
            System.out.println("Error: -path not found on command line.");
            System.exit(-1);
        }

        // Start the tool.
        repair(path, name);

    }


}
