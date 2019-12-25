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
import java.util.Collections;
import java.util.ArrayList;

/**
 * A test of concurrent insert and delete queries.
 *
 * @author Tobias Downer
 */

public class InsertDeleteTest {

    private static int thread_count = 1;


    private static void displayResult(ResultSet result_set) throws SQLException {
        PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out));
        com.pony.util.ResultOutputUtil.formatAsText(result_set, out);
        result_set.close();
        out.flush();
    }

    private static void printSyntax() {
        System.out.println(
                "Syntax: TransactionTest -url [jdbc_url] -u [username] -p [password]");
        System.out.println();
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

//      statement.executeQuery(" DROP TABLE IF EXISTS Test1 ");

            // First we create some test table,
            statement.executeQuery(
                    "  CREATE TABLE IF NOT EXISTS Test1 ( " +
                            "     cola        VARCHAR(60), " +
                            "     colb        VARCHAR(60), " +
                            "     colc        INTEGER, " +
                            "     cold        NUMERIC, " +
                            "     cole        BOOLEAN " +
                            " ) ");

            final ArrayList global_colc_list = new ArrayList();
            final ArrayList global_cold_list = new ArrayList();

            // The thread,
            class TestThread extends Thread {

                private boolean finished = false;

                private final int unique_num;

                public TestThread() {
                    unique_num = thread_count * 10000;
                    ++thread_count;
                }

                public synchronized void run() {

                    final ArrayList colc_list = new ArrayList();
                    final ArrayList cold_list = new ArrayList();

                    Connection tc;
                    try {
                        tc = DriverManager.getConnection(url, username, password);
                        tc.setAutoCommit(false);
                        PreparedStatement ts1 = tc.prepareStatement(
                                "INSERT INTO Test1 ( cola, colb, colc, cold, cole ) " +
                                        "VALUES ( ?, ?, ?, ?, ? )");
                        PreparedStatement ts2 = tc.prepareStatement(
                                "DELETE FROM Test1 WHERE colc = ? ");
                        PreparedStatement ts3 = tc.prepareStatement(
                                "SELECT cold FROM Test1 WHERE colc = ? ");
                        ResultSet r;

                        int commit_time = (int) (Math.random() * 50) + 100;

                        for (int i = 0; i < 2500; ++i) {
                            ts1.setString(1, "Yippy");
                            ts1.setString(2, "This is kind of a long string.");
                            Integer val;
                            double dval;
                            do {
                                val = (int)
                                        (Math.random() * 10000) + unique_num;
                            } while (colc_list.contains(val));
                            colc_list.add(val);
                            dval = (int) (Math.random() * 15);
                            cold_list.add(dval);

                            ts1.setInt(3, val);
                            ts1.setDouble(4, dval);
                            ts1.setBoolean(5, (Math.random() * 1000) > 500);
                            r = ts1.executeQuery();

                            if (i > 250) {
                                if (colc_list.size() != cold_list.size()) {
                                    throw new Error("non equal size lists");
                                }

                                // Pick a random entry out of the list.
                                int index = (int) (Math.random() * colc_list.size());
                                Integer v;
                                Double d;
                                v = (Integer) colc_list.get(index);
                                d = (Double) cold_list.get(index);
//                System.out.println("Random val: " + v);

                                // Does it exist?
                                ts3.setInt(1, v);
                                ResultSet r3 = ts3.executeQuery();
                                if (r3.next()) {
                                    double in_d = r3.getDouble(1);
                                    if (d != in_d) {
                                        throw new Error("Doubles not equal (" +
                                                d + " != " + in_d);
                                    }
                                    if (r3.next()) {
                                        throw new Error("Indexing error - multiple entries for: " +
                                                "colc = " + v);
                                    }
                                } else {
                                    throw new Error("No items exist for this value: colc = " +
                                            v);
                                }

                                // Delete
                                ts2.setInt(1, v);
                                ResultSet r2 = ts2.executeQuery();
                                r2.next();
                                int delete_count = r2.getInt(1);
                                if (delete_count != 1) {
                                    throw new Error("Deleted count for: colc = " +
                                            v + " is " + delete_count);
                                }

                                // Remove from our internal list
                                index = colc_list.indexOf(v);
                                colc_list.remove(index);
                                cold_list.remove(index);

                            }

                            if ((i % commit_time) == 0) {
                                System.out.print(".");
                                tc.commit();
                                commit_time = (int) (Math.random() * 50) + 100;
                            }
                        }

                        // Commit the updates.
                        tc.commit();

                        synchronized (global_colc_list) {
                            global_colc_list.addAll(colc_list);
                            global_cold_list.addAll(cold_list);
                        }

//            // Check values in the list are still in the table.
//            PreparedStatement ts4 = tc.prepareStatement(
//                                "SELECT count(*) FROM Test1 WHERE colc = ?");
//            for (int i = 0; i < colc_list.size(); ++i) {
//              int val = ((Integer) colc_list.get(i)).intValue();
//              ts4.setInt(1, val);
//              r = ts4.executeQuery();
//              if (r.next()) {
//                int count = r.getInt(1);
//                if (count != 1) {
//                  throw new Error("Query count returned '" + count +
//                                  "' and should have returned 1.");
//                }
//              }
//              else {
//                throw new Error("Value not found in table: " + val);
//              }
//            }
//            tc.commit();

                        tc.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                    finished = true;
                    notifyAll();

                }

                public synchronized void waitTillFinished() {
                    while (!finished) {
                        try {
                            wait();
                        } catch (InterruptedException e) { /* ignore */ }
                    }
                }

            }


            // Make threads
            TestThread tt1 = new TestThread();
            TestThread tt2 = new TestThread();
            TestThread tt3 = new TestThread();
            TestThread tt4 = new TestThread();
            TestThread tt5 = new TestThread();
            TestThread tt6 = new TestThread();
            TestThread tt7 = new TestThread();

            tt1.start();
            tt2.start();
            tt3.start();
            tt4.start();
            tt5.start();
//      tt6.start();
//      tt7.start();

            // Wait for threads to finish...
            tt1.waitTillFinished();
            tt2.waitTillFinished();
            tt3.waitTillFinished();
            tt4.waitTillFinished();
            tt5.waitTillFinished();
//      tt6.waitTillFinished();
//      tt7.waitTillFinished();

            Collections.sort(global_colc_list);
            Collections.sort(global_cold_list);

            // Check all values are indexed correctly...
            ResultSet sort_r1 = statement.executeQuery(
                    " select colc from Test1 order by colc ");
            int i = 0;
            while (sort_r1.next()) {
                int c_val = (Integer) global_colc_list.get(i);
                if (sort_r1.getInt(1) != c_val) {
                    System.out.println("i = " + i);
                    System.out.println("c_val = " + c_val);
                    System.out.println("value = " + sort_r1.getInt(1));
                    throw new Error("List is incorrectly ordered.");
                }
                ++i;
            }
            if (i != global_colc_list.size()) {
                throw new Error("Incorrectly numbered");
            }

            ResultSet sort_r2 = statement.executeQuery(
                    " select cold from Test1 order by cold ");
            i = 0;
            while (sort_r2.next()) {
                double d_val = (Double) global_cold_list.get(i);
                if (sort_r2.getDouble(1) != d_val) {
                    System.out.println("i = " + i);
                    System.out.println("d_val = " + d_val);
                    System.out.println("value = " + sort_r2.getDouble(1));
                    throw new Error("List is incorrectly ordered.");
                }
                ++i;
            }
            if (i != global_cold_list.size()) {
                throw new Error("Incorrectly numbered");
            }


//      try {
//        Thread.sleep(5000);
//      }
//      catch (InterruptedException e) { }


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
