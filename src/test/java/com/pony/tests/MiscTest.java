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

package com.pony.tests;

import com.pony.util.CommandLine;

import java.sql.*;
import java.io.*;
import java.util.ArrayList;

/**
 *
 *
 * @author Tobias Downer
 */

public class MiscTest {

    private static void printSyntax() {
        System.out.println(
                "Syntax: MiscTest -url [jdbc_url] -u [username] -p [password]");
        System.out.println();
    }

    private static void displayResult(ResultSet result_set) throws SQLException {
        PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out));
        com.pony.util.ResultOutputUtil.formatAsText(result_set, out);
        result_set.close();
        out.flush();
    }

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
        final String url = command_line.switchArgument("-url");
        final String username = command_line.switchArgument("-u");
        final String password = command_line.switchArgument("-p");

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
        /**
         * The global connection instance.
         */
        Connection connection;
        try {
            connection = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            System.out.println(
                    "Unable to create the database.\n" +
                            "The reason: " + e.getMessage());
            return;
        }

        // ---------- Tests start point ----------

        try {

            // Create a Statement object to execute the queries on,
            Statement statement = connection.createStatement();
            ResultSet result;

            // First we create some test table,
            statement.executeQuery(
                    "  DROP TABLE IF EXISTS MiscTest");

            // First we create some test table,
            statement.executeQuery(
                    "  CREATE TABLE MiscTest ( " +
                            "     cola        VARCHAR " +
                            " ) ");

            PreparedStatement in1 = connection.prepareStatement(
                    "INSERT INTO MiscTest ( cola ) VALUES ( ? )");

            String big_string = "bigstringbigstringbigstringbigstring";
            for (int i = 0; i < 10; ++i) {
                big_string = big_string + big_string;
            }

            in1.setString(1, big_string);
            in1.executeUpdate();

            in1.executeUpdate();

            connection.commit();

            // Close the statement and the connection.
            statement.close();
            connection.close();

        } catch (SQLException e) {
            System.out.println(
                    "An error occured\n" +
                            "The SQLException message is: " + e.getMessage());
            e.printStackTrace();
            return;
        }

    }

}

