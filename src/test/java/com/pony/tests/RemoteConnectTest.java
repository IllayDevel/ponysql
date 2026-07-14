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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Opt-in integration coverage for the old manual RemoteConnectTest program.
 */
class RemoteConnectTest {

    @Test
    void opensMultipleRemoteConnectionsWhenEnabled() throws Exception {
        assumeTrue(Boolean.getBoolean("pony.remote.tests"),
                "Set -Dpony.remote.tests=true to run remote integration tests.");

        Class.forName("com.pony.JDBCDriver");

        String url = System.getProperty(
                "pony.remote.url", "jdbc:pony://linux2");
        String user = System.getProperty("pony.remote.user", "test");
        String password = System.getProperty("pony.remote.password", "test");
        int connectionCount = Integer.getInteger(
                "pony.remote.connectionCount", 10);

        List<Connection> connections = new ArrayList<>();
        try {
            for (int i = 0; i < connectionCount; ++i) {
                connections.add(DriverManager.getConnection(
                        url, user, password));
            }
        } finally {
            for (Connection connection : connections) {
                connection.close();
            }
        }
    }

}
