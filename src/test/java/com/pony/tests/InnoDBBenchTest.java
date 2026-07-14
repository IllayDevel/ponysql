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

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Opt-in integration coverage for the old manual InnoDBBenchTest program.
 */
class InnoDBBenchTest {

    @Test
    void runsRemoteInsertBenchmarkWhenEnabled() throws Exception {
        assumeTrue(Boolean.getBoolean("pony.remote.tests"),
                "Set -Dpony.remote.tests=true to run remote integration tests.");

        Class.forName("com.pony.JDBCDriver");

        String url = System.getProperty(
                "pony.remote.url", "jdbc:pony://linux2/");
        String user = System.getProperty("pony.remote.user", "test");
        String password = System.getProperty("pony.remote.password", "test");
        int rowCount = Integer.getInteger("pony.remote.benchRows", 1000);

        try (Connection connection =
                     DriverManager.getConnection(url, user, password);
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS T1");
            statement.executeUpdate("DROP TABLE IF EXISTS T2");
            statement.executeUpdate(
                    "CREATE TABLE T1 (A INT NOT NULL, B INT, PRIMARY KEY(A))");
            statement.executeUpdate(
                    "CREATE TABLE T2 (A INT NOT NULL, B INT, PRIMARY KEY(A))");

            connection.setAutoCommit(false);
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO T1 ( A, B ) VALUES ( ?, ? )")) {
                for (int i = 0; i < rowCount; ++i) {
                    insert.setInt(1, i);
                    insert.setInt(2, i);
                    assertEquals(1, insert.executeUpdate());
                }
            }
            connection.commit();
        }
    }

}
