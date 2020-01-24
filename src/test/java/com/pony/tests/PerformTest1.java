/*
 * Pony SQL Database ( http://i-devel.ru )
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

package com.pony.tests;

import com.pony.util.CommandLine;

import java.sql.*;
import java.io.*;

/**
 *
 *
 * @author Tobias Downer
 */

public class PerformTest1 {

    private static void displayResult(ResultSet result_set) throws SQLException {
        PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out));
        com.pony.util.ResultOutputUtil.formatAsText(result_set, out);
        result_set.close();
        out.flush();
    }

    private static void printSyntax() {
        System.out.println(
                "Syntax: PerformTest1 -url [jdbc_url] -u [username] -p [password]");
        System.out.println();
    }

    /**
     * The application start.
     */
    public static void main(String[] args) {
        CommandLine command_line = new CommandLine(args);

        // Register the Pony JDBC Driver
        try {
            Class.forName("com.pony.JDBCDriver").newInstance();
        } catch (Exception e) {
            System.out.println(
                    "Unable to register the JDBC Driver.\n" +
                            "Make sure the classpath is correct.\n");
            return;
        }

        // Get the command line arguments
        String url = command_line.switchArgument("-url");
        String username = command_line.switchArgument("-u");
        String password = command_line.switchArgument("-p");

        if (url == null) {
            printSyntax();
            System.out.println("Please provide a JDBC url.");
            System.exit(-1);
        } else if (username == null || password == null) {
            printSyntax();
            System.out.println("Please provide a username and password.");
            System.exit(-1);
        }

        // Make a connection with the database.  This will create the database
        // and log into the newly created database.
        Connection connection;
        try {
            connection = DriverManager.getConnection(url, username, password);

            connection.setAutoCommit(false);
            connection.prepareStatement(
                    "CREATE TABLE perform_table ( " +
                            "  c1 INTEGER, c2 INTEGER, c3 INTEGER, c4 INTEGER )").executeQuery();

            connection.commit();

            PreparedStatement stmt = connection.prepareStatement(
                    "INSERT INTO perform_table VALUES ( ?, ?, ?, ? )");
            for (int i = 0; i < 5000; ++i) {
                stmt.setInt(1, i);
                stmt.setInt(2, -i);
                stmt.setInt(3, (i * 5) / 20);
                stmt.setInt(4, (i / 90));
                stmt.executeQuery();
                if ((i % 100) == 0) {
                    System.out.print("1");
                }
                if ((i % 1000) == 999) {
                    connection.commit();
                }
            }

            connection.commit();
            System.out.println();

            Statement w = connection.createStatement();
            for (int n = 0; n < 200; ++n) {
                w.executeQuery("SELECT * FROM perform_table ORDER BY c4, c3, c2");
                if ((n % 10) == 0) {
                    System.out.print("*");
                }
            }
            System.out.println();

        } catch (SQLException e) {
            System.out.println(
                    "An error occured\n" +
                            "The SQLException message is: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // Close the the connection.
        try {
            connection.close();
        } catch (SQLException e2) {
            e2.printStackTrace(System.err);
        }

    }


}

