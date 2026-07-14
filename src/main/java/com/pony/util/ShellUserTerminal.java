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

package com.pony.util;

/**
 * An implementation of UserTerminal that uses the shell terminal via
 * System.in and System.out.
 *
 * @author Tobias Downer
 */

public class ShellUserTerminal implements UserTerminal {

    // ---------- Implemented from UserTerminal ----------

    public void print(String str) {
        System.out.print(str);
        System.out.flush();
    }

    public void println(String str) {
        System.out.println(str);
    }

    public int ask(String question, String[] options, int default_answer) {
        // TODO
        return default_answer;
    }

}
