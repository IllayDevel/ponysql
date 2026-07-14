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
 * An interface that represents a terminal that is asked questions in human
 * and machine understandable terms, and sends answers.  This interface is
 * intended for an interface in which the user is asked questions, or for an
 * automated tool.
 *
 * @author Tobias Downer
 */

public interface UserTerminal {

    /**
     * Outputs a string of information to the terminal.
     */
    void print(String str);

    /**
     * Outputs a string of information and a newline to the terminal.
     */
    void println(String str);

    /**
     * Asks the user a question from the 'question' string.  The 'options' list
     * is the list of options that the user may select from.  The
     * 'default_answer' is the option that is selected by default.
     */
    int ask(String question, String[] options, int default_answer);

}
