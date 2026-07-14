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
import java.io.*;

public class HugeTest {
    public static Connection connection;
    public static Statement statement;
    public static ResultSet result;
    public static boolean create;

    private static void displayResult(ResultSet result_set) throws SQLException {
        PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out));
        com.pony.util.ResultOutputUtil.formatAsText(result_set, out);
        result_set.close();
        out.flush();
    }


    public static void main(String[] args) {
        try {
            if (args.length != 1) {
                System.out.println("Ussage: HugeDemo CREATE -> Create Database");
                System.out.println("        HugeDemo READ   -> Read Test");
                return;
            }
            create = args[0].equals("CREATE");

            Class.forName("com.pony.JDBCDriver").newInstance();

            String url;
            if (create)
                url = ":jdbc:pony:local://../configs/ExampleDB.conf?create=true";
            else
                url = ":jdbc:pony:local://../configs/ExampleDB.conf";

            String username = "user";
            String password = "pass1212";
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.createStatement();
            result = null;

            if (create) {
                createTable();
                fillTable();
            } else
                runDemo();

            statement.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    public static void runDemo() throws Exception {
        System.out.println("Start reading");
        result = statement.executeQuery("SELECT * FROM HUGETABLE");
        long start = System.currentTimeMillis();
        while (result.next()) {
            result.getInt("HUGETABLE.INTFIELD1");
        }
        long end = System.currentTimeMillis();
        System.out.println("Time to walk through 131 records: " + (end - start) + "ms");

        displayResult(statement.executeQuery("show status"));
    }

    public static void createTable() throws Exception {
        System.out.println("Create Table");
        int i;
        StringBuffer query = new StringBuffer();
        query.append("CREATE TABLE HUGETABLE(");
        for (i = 1; i < 140; i++) {
            query.append("INTFIELD").append(i).append(" INTEGER INDEX_NONE,");
        }

        for (i = 1; i < 80; i++) {
            query.append("DECFIELD").append(i).append(" DECIMAL(9,2) INDEX_NONE,");
        }

        for (i = 1; i < 10; i++) {
            query.append("STRINGFIELD").append(i).append(" VARCHAR(40) INDEX_NONE,");
        }
        query.append("STRINGFIELD10 VARCHAR(40) INDEX_NONE)");
        statement.execute(new String(query));
    }

    public static void fillTable() throws Exception {
        System.out.println("Filling Table");
        statement.execute("SET AUTO COMMIT OFF");
        StringBuffer query = new StringBuffer();
        int i, k;
        for (i = 1; i <= 131; i++) {
            query.setLength(0);
            query.append("INSERT INTO HUGETABLE(");
            for (k = 1; k < 140; k++) {
                query.append("HUGETABLE.INTFIELD").append(k).append(",");
            }
            for (k = 1; k < 80; k++) {
                query.append("HUGETABLE.DECFIELD").append(k).append(",");
            }
            for (k = 1; k < 9; k++) {
                query.append("HUGETABLE.STRINGFIELD").append(k).append(",");
            }
            query.append("HUGETABLE.STRINGFIELD9) VALUES(");
            for (k = 1; k < 140; k++) {
                query.append(k).append(",");
            }

            for (k = 1; k < 80; k++) {
                query.append(k).append(",");
            }

            for (k = 1; k < 9; k++) {
                query.append("'TESTSTRING").append(i).append("',");
            }
            query.append("'TESTSTRING9')");
            statement.execute(new String(query));
        }
        statement.execute("COMMIT");
    }
}

