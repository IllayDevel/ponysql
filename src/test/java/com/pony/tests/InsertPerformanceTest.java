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

/**
 *
 *
 * @author Tobias Downer
 */

public class InsertPerformanceTest {

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
            System.out.println("Please provide a JDBC url.");
            System.exit(-1);
        } else if (username == null || password == null) {
            System.out.println("Please provide a username and password.");
            System.exit(-1);
        }

        // Make a connection with the database.  This will create the database
        // and log into the newly created database.
        try {
            Connection connection = DriverManager.getConnection(url, username, password);

            Statement stmt = connection.createStatement();
            stmt.executeQuery(
                    "CREATE TABLE TTable ( c1 NUMERIC, c2 NUMERIC, c3 TEXT, c4 TEXT )");

            PreparedStatement ins = connection.prepareStatement(
                    "INSERT INTO TTable ( c1, c2, c3, c4 ) VALUES ( ?, ?, ?, ? )");

            final int insert_count = 20000;
            long t_in = System.currentTimeMillis();

            connection.setAutoCommit(false);
            for (int i = 0; i < insert_count; ++i) {
                ins.setInt(1, i);
                ins.setInt(2, i + 10000);
                ins.setString(3, "" + i + " - a string");
                ins.setString(4, "This is some data being inserted.");
                ins.executeUpdate();

                if ((i % 500) == 499) {
                    connection.commit();
                }
            }

            connection.commit();

            long t_out = System.currentTimeMillis();
            long t_taken = t_out - t_in;

            System.out.println("Time taken: " + t_taken + "ms.");
            System.out.println("Record count: " + insert_count);
            System.out.println("Per Second: " + ((float) insert_count / ((float) t_taken / 1000)));

            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}

