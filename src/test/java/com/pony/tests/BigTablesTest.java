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

import com.pony.database.control.*;

import java.io.*;
import java.sql.*;

/**
 * Database test that creates a massive (but practical) table.
 *
 * @author Tobias Downer
 */

public class BigTablesTest {


    public static long compareStreams(InputStream in1, InputStream in2,
                                      long stream_size) throws IOException {
        boolean end_reached = false;
        long read_count = 0;
        while (!end_reached) {
            int val1 = in1.read();
            int val2 = in2.read();
            if (val1 != val2) {
                throw new Error("Blob streams do not compare correctly.");
            }
            if (val1 == -1) {
                end_reached = true;
            }
            ++read_count;
        }

        --read_count;
        if (read_count != stream_size) {
            throw new Error("Stream size mismatch.  Expecting " + stream_size +
                    " but actually read " + read_count);
        }

        return read_count;
    }


    public static void main(String[] args) {

        // Get the database controller from the com.pony.database.control API
        DBController controller = DBController.getDefault();

        // Make a database configuration object and set the paths of the database
        // and log files.
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath("./data");
        config.setLogPath("./log");

        DBSystem database;

        // Does the database exist already?
        if (controller.databaseExists(config)) {
            // Yes so report the error
            throw new RuntimeException("Database already exists.");
        }

        // Now create the database
        database = controller.createDatabase(config, "test", "test");

        try {
            // Make a JDBC connection to the database
            Connection connection = database.getConnection("test", "test");

            // The database meta data
            DatabaseMetaData meta_data = connection.getMetaData();
            System.out.println(meta_data.getDatabaseProductName());

            connection.setAutoCommit(false);

            // Create a table.
            Statement stmt = connection.createStatement();
            stmt.executeQuery("CREATE TABLE AccountLog ( " +
                    " \"id\" NUMERIC, " +
                    " \"v1\" NUMERIC INDEX_NONE, " +
                    " \"v2\" NUMERIC INDEX_NONE, " +
                    " \"v3\" NUMERIC INDEX_NONE, " +
                    " \"data\" BINARY INDEX_NONE )");

            connection.commit();

            // Start logging the history of a lot of accounts
            PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO AccountLog VALUES ( ?, ?, ?, ?, ? )");
            PreparedStatement select = connection.prepareStatement(
                    "SELECT * FROM AccountLog WHERE \"id\" = ?");

            long tran_mults = 50;
            long each_commit = 125;

            System.out.println("Simulating " + (tran_mults * each_commit) +
                    " simple transactions.");

            long acct_seed = 1;
            long amnt_seed = 1;


            for (int n = 0; n < tran_mults; ++n) {
                System.out.print(".");
                for (int i = 0; i < each_commit; ++i) {
                    acct_seed = ((acct_seed + 40031) * 541) % 147;
                    amnt_seed = ((amnt_seed * 65543) + 23) % 2414779;

                    int v1 = ((int) (Math.random() * 100000)) + 150000;
                    int v2 = (int) ((acct_seed & 15) + 2);
                    int v3 = (int) ((amnt_seed & 255) + 2);

                    insert.setLong(1, ((long) n * each_commit) + i);
                    insert.setInt(2, v1);
                    insert.setInt(3, v2);
                    insert.setInt(4, v3);
                    insert.setBinaryStream(5,
                            new LargeAlgorithmicBinaryStream(v1, v2, v3), v1);

                    insert.executeUpdate();
                }
                connection.commit();

                // Check everything we committed.
                System.out.print("C");
                for (int i = 0; i < each_commit; ++i) {
                    if ((int) (Math.random() * 5) == 0) {
                        select.setLong(1, ((long) n * each_commit) + i);
                        ResultSet rs = select.executeQuery();
                        rs.next();
                        int v1 = rs.getInt(2);
                        int v2 = rs.getInt(3);
                        int v3 = rs.getInt(4);
                        InputStream bin = rs.getBinaryStream(5);

                        // Compare to check the data stored is the same as expected.
                        compareStreams(bin,
                                new LargeAlgorithmicBinaryStream(v1, v2, v3), v1);

                        bin.close();
                        rs.close();
                    }
                }

            }

        } catch (SQLException e) {
            e.printStackTrace(System.err);
        } catch (IOException e) {
            e.printStackTrace(System.err);
        }

        // Close the database
        database.close();

        // Print the memory usage.
        System.gc();
        System.out.println("Total memory: " + Runtime.getRuntime().totalMemory());
        System.out.println("Free memory:  " + Runtime.getRuntime().freeMemory());

    }


    /**
     * A simple InputStream implementation that iterates across a seeded
     * algorithmic pattern of the given size.
     */
    private static class LargeRandomBinaryStream
            extends LargeAlgorithmicBinaryStream {

        public LargeRandomBinaryStream(int max_size) {
            super(max_size, 3, 100);
        }

    }

    /**
     * A simple InputStream implementation that iterates across a seeded
     * algorithmic pattern of the given size.
     */
    private static class LargeAlgorithmicBinaryStream extends InputStream {

        final int max_size;
        int index;

        final int param1;
        final int param2;

        public LargeAlgorithmicBinaryStream(int max_size, int param1, int param2) {
            this.max_size = max_size;
            this.index = 0;
            this.param1 = param1;
            this.param2 = param2;
        }

        public int read() throws IOException {
            // If end of stream reached.
            if (index == max_size) {
                return -1;
            }
            // The idea here is to create a sequence of numbers that has very large
            // repeat cycle.
            int val = ((index * (param1 + (index / param2))) & 0x0FF);
            ++index;
            return val;
        }

    }

}

