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

package com.pony.runtime;

import com.pony.database.control.DefaultDBConfig;
import com.pony.database.control.TCPJDBCServer;
import com.pony.database.control.DBController;
import com.pony.database.control.DBConfig;
import com.pony.database.control.DBSystem;
import com.pony.util.CommandLine;
import com.pony.database.global.StandardMessages;

import java.io.IOException;
import java.io.File;
import java.sql.*;

/**
 * The start point of the Pony SQL database server.  This application is
 * used as a command-line tool for creating and booting a database.
 *
 * @author Tobias Downer
 */

public class PonyDBMain {

    /**
     * Print the syntax.
     */
    private static void printSyntax() {
        System.out.println(
                "Command Line Arguments:\n" +
                        "\n" +
                        "-- Configuration --\n" +
                        "\n" +
                        "  -conf [database_config_file]\n" +
                        "    The database configuration file to use.  If not specified then it searches\n" +
                        "    for 'db.conf' in the current directory.\n" +
                        "  -dbpath [database_data_path]\n" +
                        "    Specifies where the database data files are located.\n" +
                        "  -logpath [log_path]\n" +
                        "    Specifies where the logs are to be kept.\n" +
                        "  -jdbcaddress [address]\n" +
                        "    For multi-homed machines, allows for the database to bind to a particular\n" +
                        "    host address.\n" +
                        "  -jdbcport [port_number]\n" +
                        "    Sets the TCP port where the JDBC clients must connect to.\n" +
                        "  -C[key]=[value]\n" +
                        "    Where [key] is a configuration property and [value] is a value to set the\n" +
                        "    property to.  This can be used to override any property in the config.\n" +
                        "    file.  Example: -Cmaximum_worker_threads=2\n" +
                        "\n" +
                        "-- Functions --\n" +
                        "\n" +
                        "  -create [admin_username] [admin_password]\n" +
                        "    Creates an empty database and adds a user with the given username and\n" +
                        "    password with complete privs.  This will not start the database server.\n" +
                        "  -shutdown [host] [port] [admin_username] [admin_password]\n" +
                        "    Shuts down the database server running on the host/port.  [host] and\n" +
                        "    [port] are optional, they default to 'localhost' and port 9157.\n" +
                        "  -boot\n" +
                        "    Boots the database server from the information given in the configuration\n" +
                        "    file.  This switch is implied if no other function switch is provided.\n" +
                        "\n" +
                        "Examples:\n" +
                        "\n" +
                        "  PonyDBMain -create admuser ad944\n" +
                        "    == Creates a new database with admin username 'admuser' and admin password\n" +
                        "       'ad944'.\n" +
                        "  PonyDBMain -conf /home/pony/db.conf\n" +
                        "    == Boots a database from the configuration file found at\n" +
                        "       /home/pony/db.conf\n" +
                        "  PonyDBMain -conf db.conf -dbpath /home/myapp/data\n" +
                        "    == Boots a database and specifies that the database data path is found at\n" +
                        "       /home/myapp/data (overriding the path set in the configuration file).\n"
        );
    }

    /**
     * Performs the create command.
     */
    private static void doCreate(String database_name,
                                 String username, String password,
                                 DBConfig config) {

        DBController controller = DBController.getDefault();
        // Create the database with the given configuration then close it
        DBSystem database = controller.createDatabase(config, username, password);
        database.close();

    }

    /**
     * Performs the shutdown command.
     */
    private static void doShutDown(String host, String port,
                                   String username, String password) {

        // Actually - config bundle useless for this....
        Connection connection;
        try {
            Class.forName("com.pony.JDBCDriver").newInstance();

            String url = ":jdbc:pony://" + host + ":" + port + "/";
            connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return;
        }

        try {
            Statement statement = connection.createStatement();
            ResultSet result_set = statement.executeQuery("SHUTDOWN");
            statement.close();
        } catch (SQLException e) {
            System.out.println("Unable to shutdown database: " + e.getMessage());
        }

        try {
            connection.close();
        } catch (SQLException e) {
            System.out.println("Unable to close connection: " + e.getMessage());
        }

    }

    /**
     * Performs the boot command.
     */
    private static void doBoot(DBConfig conf) {

        DBController controller = DBController.getDefault();
        // Start the database in the local JVM
        DBSystem database = controller.startDatabase(conf);
        // Connect a TCPJDBCServer to it.
        TCPJDBCServer server = new TCPJDBCServer(database);
        // And start the server
        server.start();

        // Output a message telling us about the server
        System.out.print(server.toString());
        System.out.println(".");

    }

    /**
     * The Pony Database application starting point.
     */
    public static void main(String[] args) {

        CommandLine command_line = new CommandLine(args);

        // Print the startup message,
        System.out.println();
        System.out.println(StandardMessages.NAME);
        System.out.println(StandardMessages.COPYRIGHT);
        System.out.println("Use: -h for help.");

        System.out.println("\n" +
                "  Pony SQL Database comes with ABSOLUTELY NO WARRANTY.\n" +
                "  This is free software, and you are welcome to redistribute it\n" +
                "  under certain conditions.  See LICENSE for details of the\n" +
                "  Apache License.\n");

        // Print help?
        if (command_line.containsSwitchFrom("-h,--help,/?")) {
            printSyntax();
            return;
        }

        // The name of the database
        String database_name = "DefaultDatabase";

        if (command_line.containsSwitch("-shutdown")) {
            // Try to match the shutdown switch.
            String[] sd_parm = command_line.switchArguments("-shutdown", 4);
            if (sd_parm == null) {
                sd_parm = command_line.switchArguments("-shutdown", 3);
                if (sd_parm == null) {
                    sd_parm = command_line.switchArguments("-shutdown", 2);
                    if (sd_parm != null) {
                        doShutDown("localhost", "9157", sd_parm[0], sd_parm[1]);
                        return;
                    } else {
                        System.out.println("Incorrect '-shutdown' format.");
                        return;
                    }
                } else {
                    doShutDown(sd_parm[0], "9157", sd_parm[1], sd_parm[2]);
                    return;
                }
            } else {
                doShutDown(sd_parm[0], sd_parm[1], sd_parm[2], sd_parm[3]);
                return;
            }
        }

        // Get the conf file if applicable.
        String conf_file = command_line.switchArgument("-conf", "./db.conf");

        // Extract the root part of the configuration path.  This will be the root
        // directory.
        File absolute_config_path = new File(
                new File(conf_file).getAbsolutePath());
        File root_path = new File(absolute_config_path.getParent());
        // Create a default DBConfig object
        DefaultDBConfig config = new DefaultDBConfig(root_path);
        try {
            config.loadFromFile(new File(conf_file));
        } catch (IOException e) {
            System.out.println(
                    "Error: configuration file '" + conf_file + "' was not found.");
            System.out.println();
            System.exit(1);
        }

        // Any configuration overwritten switches?
        String[] cparam = command_line.switchArguments("-dbpath", 1);
        if (cparam != null) {
            config.setValue("database_path", cparam[0]);
        }
        cparam = command_line.switchArguments("-logpath", 1);
        if (cparam != null) {
            config.setValue("log_path", cparam[0]);
        }
        cparam = command_line.switchArguments("-jdbcaddress", 1);
        if (cparam != null) {
            config.setValue("jdbc_server_address", cparam[0]);
        }
        cparam = command_line.switchArguments("-jdbcport", 1);
        if (cparam != null) {
            config.setValue("jdbc_server_port", cparam[0]);
        }
        // Find all '-C*' style switches,
        String[] c_args = command_line.allSwitchesStartingWith("-C");
        for (int i = 0; i < c_args.length; ++i) {
            if (c_args[i].length() > 2) {
                String c_arg = c_args[i].substring(2);
                int split_point = c_arg.indexOf("=");
                if (split_point > 0) {
                    String key = c_arg.substring(0, split_point);
                    String value = c_arg.substring(split_point + 1);
                    config.setValue(key, value);
                } else {
                    System.out.println("Ignoring -C switch: '" + c_arg + "'");
                }
            }
        }

        // Try to match create switch.
        String[] create_parm = command_line.switchArguments("-create", 2);
        if (create_parm != null) {
            doCreate(database_name, create_parm[0], create_parm[1],
                    config);
            return;
        }

        // Log the start time.
        long start_time = System.currentTimeMillis();

        // Nothing matches, so we must be wanting to boot a new server
        doBoot(config);

        long count_time = System.currentTimeMillis() - start_time;
        System.out.println("Boot time: " + count_time + "ms.");

        return;
    }

}
