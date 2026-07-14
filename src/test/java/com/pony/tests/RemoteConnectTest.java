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

public class RemoteConnectTest {

    public static void main(String[] args) {

        try {
            Class.forName("com.pony.JDBCDriver");

            System.out.println("STARTED");

            Connection[] c = new Connection[50];

            for (int i = 0; i < 50; ++i) {
                c[i] =
                        DriverManager.getConnection("jdbc:pony://linux2", "test", "test");
            }

            System.out.println("FINISHED");

            Thread.sleep(10000);


        } catch (Throwable e) {
            e.printStackTrace();
        }

    }

}

