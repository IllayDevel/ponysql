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
 * A class that runs various tests using the Pony JDBC interface.
 *
 * @author Tobias Downer
 */

public class JDBCTests {

    /**
     * The global connection instance.
     */
    private static Connection connection;
    private static Connection connection1;
    private static Connection connection2;
    private static Connection connection3;


    private static void displayResult(ResultSet result_set) throws SQLException {
        PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out));
        com.pony.util.ResultOutputUtil.formatAsText(result_set, out);
        result_set.close();
        out.flush();
    }

    private static void printSyntax() {
        System.out.println(
                "Syntax: JDBCTests -url [jdbc_url] -u [username] -p [password]");
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
        try {
            connection = DriverManager.getConnection(url, username, password);
            connection1 = DriverManager.getConnection(url, username, password);
            connection2 = DriverManager.getConnection(url, username, password);
            connection3 = DriverManager.getConnection(url, username, password);

////      connection1 = connection;
//      connection2 = connection;
//      connection3 = connection1;
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

            // Set auto-commit off.
            connection.setAutoCommit(false);
            connection1.setAutoCommit(false);
            connection2.setAutoCommit(false);
            connection3.setAutoCommit(false);

            boolean finished = false;
            while (!finished) {

//      Statement statement = connection.createStatement();
//      statement.executeQuery(
//	  " DROP TABLE IF EXISTS Test1, Test2, Test3, Test4, Test5 ");

                // First we create some test tables,
                statement.executeQuery(
                        "  CREATE TABLE IF NOT EXISTS Test1 ( " +
                                "     id          VARCHAR(200), " +
                                "     rand_val    INTEGER, " +
                                "     binary_col  BINARY, " +
                                "     string_col  VARCHAR(1048576) ) ");

                statement.executeQuery(
                        "  CREATE TABLE IF NOT EXISTS Test2 ( " +
                                "     id          VARCHAR(200), " +
                                "     rand_val    INTEGER, " +
                                "     binary_col  BINARY, " +
                                "     string_col  VARCHAR(1048576) ) ");

                statement.executeQuery(
                        "  CREATE TABLE IF NOT EXISTS Test3 ( " +
                                "     id          VARCHAR(200), " +
                                "     rand_val    INTEGER, " +
                                "     binary_col  BINARY, " +
                                "     string_col  VARCHAR(1048576) ) ");

                statement.executeQuery(
                        "  CREATE TABLE IF NOT EXISTS Test4 ( " +
                                "     id          VARCHAR(200), " +
                                "     rand_val    INTEGER, " +
                                "     binary_col  BINARY, " +
                                "     string_col  VARCHAR(1048576) ) ");

                statement.executeQuery(
                        "  CREATE TABLE IF NOT EXISTS Test5 ( " +
                                "     id          VARCHAR(200), " +
                                "     rand_val    INTEGER, " +
                                "     binary_col  BINARY, " +
                                "     string_col  VARCHAR(1048576) ) ");
                connection.commit();

                // Lets run some concurrent inserts,

                Runnable runner = new Runnable() {
                    public void run() {
                        try {

//            try {
//              Thread.sleep(3000);
//            }
//            catch (InterruptedException e) { }

                            Statement my_statement = connection.createStatement();

                            // Create a Statement object to execute the queries on,
                            PreparedStatement s = connection.prepareStatement(
                                    "INSERT INTO Test1 ( id, rand_val, string_col ) VALUES ( ?, ?, ? )");

//            for (int n = 0; n < 15; ++n) {
//              for (int i = 0; i < 50; ++i) {
//                s.setString(1, "[Thread1] (1)" + n + ":" + i);
//                s.setInt(2, (int) (Math.random() * 3000) + 1000);
//                s.setString(3, "A test string!");
//                s.execute();
//                System.out.print("1");
//              }
//              connection.commit();
//              for (int p = 0; p < 1; ++p) {
//                my_statement.executeQuery(" SELECT COUNT(*) FROM Test1 WHERE string_col != 'A' AND rand_val > -999 ");
//                System.out.print("CQ");
//                connection.commit();
//              }
////              my_statement.executeQuery(" UPDATE Test1 SET string_col = CONCAT(string_col, '1') ");
////              System.out.print("UQ");
//            }

                            for (int n = 0; n < 250; ++n) {
                                my_statement.executeQuery(" SELECT COUNT(*) FROM Test1 WHERE string_col != 'A' AND rand_val > -999 ");
                                System.out.print("1CQ");
                                connection.commit();
                            }


                            s.close();

                            connection.commit();
                            System.out.print("1*  Reading from Table1 *");
                            // Reading from table 1,
                            s = connection.prepareStatement(
                                    "  SELECT * FROM Test1 " +
                                            "   WHERE id != 'A' AND string_col != 'B' " +
                                            "ORDER BY id");
                            ResultSet r = s.executeQuery();
                            int count = 0;
                            while (r.next()) {
                                ++count;
                            }
                            System.out.print("1*  Read " + count +
                                    " from Table 1 complete *");
                            r.close();
                            connection.commit();

                            s = connection.prepareStatement(
                                    "INSERT INTO Test1 ( id, rand_val, string_col ) VALUES ( ?, ?, ? )");

                            for (int n = 0; n < 19; ++n) {
                                for (int i = 0; i < 80; ++i) {
                                    s.setString(1, "[Thread1] (2)" + n + ":" + i);
                                    s.setInt(2, (int) (Math.random() * 5000));
                                    s.setString(3, "A test string!");
                                    s.execute();
                                    System.out.print("1");
                                }
                                System.out.print("1C");
                                connection.commit();
                            }

                            s.close();

                            System.out.print("1E");
                        } catch (SQLException e) {
                            System.out.println("END Runner 1");
                            e.printStackTrace();
                        }
                    }
                };

                Runnable runner2 = new Runnable() {
                    public void run() {
                        try {
                            // Create a Statement object to execute the queries on,
                            PreparedStatement s = connection1.prepareStatement(
                                    "INSERT INTO Test2 ( id, rand_val, string_col ) VALUES ( ?, ?, ? )");
                            for (int i = 0; i < 2500; ++i) {
                                s.setString(1, "[Thread2] " + i);
                                s.setInt(2, (int) (Math.random() * 4000));
                                s.setString(3, "A test string!");
                                s.execute();
                                System.out.print("2");
                                if ((i % 100) == 0) {
                                    connection1.commit();
                                    System.out.print("2C");
                                }
                            }
                            connection1.commit();

                            s.close();

                            System.out.print("2C");
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                };

                Runnable runner3 = new Runnable() {
                    public void run() {
                        try {
                            // Create a Statement object to execute the queries on,
                            PreparedStatement s = connection2.prepareStatement(
                                    "INSERT INTO Test1 ( id, rand_val, string_col ) VALUES ( ?, ?, ? )");
//            for (int i = 0; i < 2500; ++i) {
//              s.setString(1, "[Thread3] " + i);
//              s.setInt(2, (int) (Math.random() * 4000));
//              s.setString(3, "A test string!");
//              s.execute();
//              System.out.print("3");
//            }
//            connection2.commit();

                            for (int n = 0; n < 250; ++n) {
                                s.executeQuery(" SELECT COUNT(*) FROM Test1 WHERE string_col != 'A' AND rand_val > -999 ");
                                System.out.print("3CQ");
                                connection2.commit();
                            }

                            s.close();

                            System.out.print("3C");
                        } catch (SQLException e) {
                            System.out.println("END Runner 3");
                            e.printStackTrace();
                        }
                    }
                };

                Runnable runner4 = new Runnable() {
                    public void run() {
                        try {
                            // Create a Statement object to execute the queries on,
                            PreparedStatement s = connection3.prepareStatement(
                                    "INSERT INTO Test1 ( id, rand_val, string_col ) VALUES ( ?, ?, ? )");
                            for (int i = 0; i < 2000; ++i) {
                                s.setString(1, "[Thread4] " + i);
                                s.setInt(2, (int) (Math.random() * 2000));
                                s.setString(3, "A test string!");
                                s.execute();
                                System.out.print("4");
                                if ((i % 123) == 0) {
                                    connection3.commit();
                                    System.out.print("4C");
                                }
                            }

                            connection3.commit();
                            System.out.print("4U");
                            // When this has finished, update everything in Test2...
                            s = connection3.prepareStatement(
                                    "UPDATE Test1 " +
                                            " SET string_col = CONCAT(string_col, '--An post thing') " +
                                            " WHERE string_col = 'A test string!' ");
                            s.execute();
                            System.out.print("4UE");
                            connection3.commit();

                            s.close();

                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                };

                Runnable runner5 = new Runnable() {
                    public void run() {
                        try {
                            // Create a Statement object to execute the queries on,
                            PreparedStatement s = connection3.prepareStatement(
                                    "INSERT INTO Test1 ( id, rand_val, string_col ) VALUES ( ?, ?, ? )");
                            for (int i = 0; i < 2500; ++i) {
                                s.setString(1, "[Thread5] " + i);
                                s.setInt(2, (int) (Math.random() * 1000));
                                s.setString(3, "A test string!");
                                s.execute();
                                System.out.print("5");
                                if ((i % 243) == 0) {
                                    connection3.commit();
                                    System.out.print("5C");
                                }
                            }
                            connection3.commit();
                            System.out.print("5E");

                            s.close();

                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                };

                Runnable runner6 = new Runnable() {
                    public void run() {
                        try {
                            // Create a Statement object to execute the queries on,
                            PreparedStatement s = connection1.prepareStatement(
                                    "INSERT INTO Test2 ( id, rand_val, string_col ) VALUES ( ?, ?, ? )");
                            for (int i = 0; i < 1000; ++i) {
                                s.setString(1, "[Thread6] " + i);
                                s.setInt(2, (int) (Math.random() * 2000));
                                s.setString(3, "A test string!");
                                s.execute();
                                System.out.print("6");
                                if ((i % 298) == 0) {
                                    connection1.commit();
                                    System.out.print("6C");
                                }
                            }

                            System.out.print("6U");
                            connection1.commit();
                            // When this has finished, update everything in Test2...
                            s = connection1.prepareStatement(
                                    "UPDATE Test2 " +
                                            " SET string_col = CONCAT(string_col, '--An post thing') " +
                                            " WHERE string_col = 'A test string!' ");
                            s.execute();
                            connection1.commit();
                            System.out.print("6UE");

                            s.close();

                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                };


                Worker worker = new Worker(runner);
                Worker worker2 = new Worker(runner2);
                Worker worker3 = new Worker(runner3);
                Worker worker4 = new Worker(runner4);
                Worker worker5 = new Worker(runner5);
                Worker worker6 = new Worker(runner6);

                worker.start();
                worker3.start();
                worker4.start();
                worker5.start();
                worker6.start();
                worker2.start();

                worker.waitTillFinished();
                worker2.waitTillFinished();
                worker3.waitTillFinished();
                worker4.waitTillFinished();
                worker5.waitTillFinished();
                worker6.waitTillFinished();


                System.out.println("\n--- All Tests Complete ---");

                connection.commit();
                connection1.commit();
                connection2.commit();
                connection3.commit();

                displayResult(statement.executeQuery("show status"));
//      displayResult(statement.executeQuery("show connections"));

                ResultSet index_check = statement.executeQuery("select rand_val, string_col from Test1 where string_col = 'A test string!' order by rand_val");
                StringBuffer buf = new StringBuffer();
                int old_val = -1;
                while (index_check.next()) {
                    int val = index_check.getInt(1);
                    buf.append(val);
                    buf.append(", ");
//        System.out.print(val);
//        System.out.print(", ");
                    if (val < old_val) {
                        System.out.println(new String(buf));
                        throw new Error("List not sorted - error with indexing! (" +
                                val + " < " + old_val + ")");
                    }
                    old_val = val;
                }
                index_check.close();
                System.out.println();

                displayResult(statement.executeQuery("show status"));

                // Drop from all tables, then repeat.
                statement.executeQuery("delete from Test1");
                statement.executeQuery("delete from Test2");
                connection.commit();

                try {
                    // The purpose of doing a GC here is to wipe any hanging statements
                    // and result sets.  The GC will close the ResultSet/Statement as
                    // part of finalization.

                    // Wait 10 seconds just for various clean ups to happen, then repeat.
                    // Without this, data files grow huge because row garbage collector
                    // doesn't get a chance to kick in.
                    System.gc();
                    Thread.sleep(5000);
                    System.gc();
                    Thread.sleep(5000);
                } catch (InterruptedException e) { /* ignore */ }

            } // while (true)


            // Close the statement and the connection.
            statement.close();
            connection.close();
            connection1.close();
            connection2.close();
            connection3.close();

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

    // ---------- Inner classes ----------

    /**
     * A worker thread that runs a 'Runnable'.
     */
    private static class Worker extends Thread {

        final Runnable runner;
        boolean finished = false;

        Worker(Runnable runner) {
            this.runner = runner;
        }

        public void run() {
            runner.run();
            synchronized (this) {
                finished = true;
                notifyAll();
            }
        }

        public synchronized void waitTillFinished() {
            while (!finished) {
                try {
                    wait();
                } catch (InterruptedException e) { /* ignore */ }
            }
        }

    }


}
