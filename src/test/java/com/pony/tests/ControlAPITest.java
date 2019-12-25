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

import java.io.File;
import java.sql.*;

/**
 *
 *
 * @author Tobias Downer
 */

public class ControlAPITest {

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
            // Yes, so delete it
            database = controller.startDatabase(config);
            database.setDeleteOnClose(true);
            database.close();
        }

        // Now create the database
        database = controller.createDatabase(config, "test", "test");
        database.setDeleteOnClose(true);

        try {
            // Make a JDBC connection to the database
            Connection connection = database.getConnection("test", "test");

            // The database meta data
            DatabaseMetaData meta_data = connection.getMetaData();
            System.out.println(meta_data.getDatabaseProductName());

            connection.setAutoCommit(false);

            // Create a table.
            Statement stmt = connection.createStatement();
            stmt.executeQuery("CREATE TABLE test ( a INTEGER, b TEXT )");

            // Insert 100 records into the table.
            PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO test ( a, b ) VALUES ( ?, ? )");
            for (int i = 1; i <= 100; ++i) {
                insert.setInt(1, i);
                insert.setString(2, "record:" + i);
                insert.executeUpdate();
            }

            // Commit the changes
            connection.commit();

            // Simple select on the table.
            ResultSet rs = stmt.executeQuery(
                    "SELECT a, b FROM test WHERE a <= 10 OR a >= 60");
            while (rs.next()) {
                int a_col = rs.getInt(1);
                String b_col = rs.getString(2);
                System.out.println(a_col + ", " + b_col);
            }

            rs.close();
            connection.close();

        } catch (SQLException e) {
            e.printStackTrace(System.err);
        }

        File[] list = new File("./data").listFiles();
        System.out.println("Files in data directory before close:");
        for (File value : list) {
            System.out.println(value.toString());
        }

        // Close the database
        database.close();

        list = new File("./data").listFiles();
        System.out.println("Files in data directory after close:");
        for (File file : list) {
            System.out.println(file.toString());
        }

        // Print the memory usage.
        System.gc();
        System.out.println("Total memory: " + Runtime.getRuntime().totalMemory());
        System.out.println("Free memory:  " + Runtime.getRuntime().freeMemory());

        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
