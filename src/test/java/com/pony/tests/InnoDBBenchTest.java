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

import java.sql.*;

/**
 *
 *
 * @author Tobias Downer
 */

public class InnoDBBenchTest {

    public static void main(String[] args) {

        try {
            Class.forName("com.pony.JDBCDriver");
        } catch (Exception e) {
            throw new Error("No JDBC driver in classpath!");
        }

        try {
            Connection c =
                    DriverManager.getConnection("jdbc:pony://linux2/", "test", "test");

            c.createStatement().executeQuery(
                    "CREATE TABLE T1 (A INT NOT NULL, B INT, PRIMARY KEY(A))");
            c.createStatement().executeQuery(
                    "CREATE TABLE T2 (A INT NOT NULL, B INT, PRIMARY KEY(A))");

            PreparedStatement insert_stmt = c.prepareStatement(
                    "INSERT INTO T1 ( A, B ) VALUES ( ?, ? )");

            c.setAutoCommit(false);
            long start_time = System.currentTimeMillis();
            for (int i = 0; i < 100000; ++i) {
                insert_stmt.setInt(1, i);
                insert_stmt.setInt(2, i);
                insert_stmt.executeQuery();
                if ((i % 10000) == 0) {
                    System.out.print(".");
                    System.out.flush();
                }
            }
            c.commit();

            long total_time = System.currentTimeMillis() - start_time;
            System.out.println("Total time: " + total_time);
            System.out.println("Inserts per sec: " + (total_time / 100000));

            c.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
