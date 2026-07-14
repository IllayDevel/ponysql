/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
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
