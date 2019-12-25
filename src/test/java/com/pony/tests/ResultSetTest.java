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

/**
 * com.pony.tests.ResultSetTest  25 Feb 2001
 * <p>
 * {{@INCLUDE LICENSE}}
 */

package com.pony.tests;

import com.pony.util.CommandLine;

import java.sql.*;
import java.io.*;
import java.util.ArrayList;

/**
 * A test of the ResultSet.
 *
 * @author Tobias Downer
 */

public class ResultSetTest {

    private static void printSyntax() {
        System.out.println(
                "Syntax: ResultSetTest -url [jdbc_url] -u [username] -p [password]");
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

            // Display information about the database,
            DatabaseMetaData db_meta = connection.getMetaData();
            String name = db_meta.getDatabaseProductName();
            String version = db_meta.getDatabaseProductVersion();

            System.out.println("Database Product Name: " + name);
            System.out.println("Database Product Version: " + version);

            // Create a Statement object to execute the queries on,
            Statement statement = connection.createStatement();
            ResultSet result;

            // First we create some test table,
            statement.executeQuery(
                    "  DROP TABLE IF EXISTS RSTest");

            // First we create some test table,
            statement.executeQuery(
                    "  CREATE TABLE IF NOT EXISTS RSTest ( " +
                            "     cola        VARCHAR(60), " +
                            "     colb        VARCHAR(60), " +
                            "     colc        INTEGER, " +
                            "     cold        NUMERIC, " +
                            "     cole        BOOLEAN " +
                            " ) ");

            PreparedStatement ts1 = connection.prepareStatement(
                    "INSERT INTO RSTest ( cola, colb, colc, cold, cole ) " +
                            "VALUES ( ?, ?, ?, ?, ? )");


            ResultSet r;

            ts1.setString(1, "Bah1");
            ts1.setString(2, "Bah2");
            ts1.setInt(3, 5);
            ts1.setDouble(4, 90.55);
            ts1.setBoolean(5, true);
            ts1.executeUpdate();
            ts1.setString(1, "Bah1");
            ts1.setString(2, "Bah2");
            ts1.setInt(3, 6);
            ts1.setDouble(4, 90.55);
            ts1.setBoolean(5, true);
            ts1.executeUpdate();
            ts1.setString(1, "Bah1");
            ts1.setString(2, "Bah2");
            ts1.setInt(3, 7);
            ts1.setDouble(4, 90.55);
            ts1.setBoolean(5, true);
            ts1.executeUpdate();
            ts1.setString(1, "Bah1");
            ts1.setString(2, "Bah2");
            ts1.setInt(3, 8);
            ts1.setDouble(4, 90.55);
            ts1.setBoolean(5, true);
            ts1.executeUpdate();

            PreparedStatement ts2 = connection.prepareStatement(
                    "DELETE FROM RSTest WHERE colc = ? ");

            ts2.setInt(1, 6);
            r = ts2.executeQuery();
            r.next();
            System.out.println(r.getInt(1));
            ts2.setInt(1, 7);
            r = ts2.executeQuery();
            r.next();
            System.out.println(r.getInt(1));
            ts2.setInt(1, 10);
            r = ts2.executeQuery();
            r.next();
            System.out.println(r.getInt(1));

            System.out.println("\n--- All Tests Complete ---");

            connection.commit();

            displayResult(statement.executeQuery("show status"));
            displayResult(statement.executeQuery("show connections"));

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

//    // Close the the connection.
//    try {
//      connection.close();
//    }
//    catch (SQLException e2) {
//      e2.printStackTrace(System.err);
//    }

    }


}
