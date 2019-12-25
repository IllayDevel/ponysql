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

package com.pony.util;

import java.util.Vector;
import java.util.StringTokenizer;

/**
 * Used to parse a command-line.
 *
 * @author Tobias Downer
 */

public class CommandLine {

    /**
     * The command line arguments.
     */
    private final String[] args;

    /**
     * Constructs the command line parser from the String[] array passed as the
     * argument to the application.
     */
    public CommandLine(String[] args) {
        if (args == null) {
            args = new String[0];
        }
        this.args = args;
    }

    /**
     * Returns true if the switch is in the command line.
     * eg. command_line.containsSwitch("--help");
     */
    public boolean containsSwitch(String switch_str) {
        for (String arg : args) {
            if (arg.equals(switch_str)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Given a comma deliminated list, this scans for one of the switches in the
     * list.  eg. command_line.containsSwitchFrom("--help,-help,-h");
     */
    public boolean containsSwitchFrom(String switch_str) {
        StringTokenizer tok = new StringTokenizer(switch_str, ",");
        while (tok.hasMoreElements()) {
            String elem = tok.nextElement().toString();
            if (containsSwitch(elem)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if the command line contains a switch starting with the
     * given string.  eg. command_line.containsSwitchStart("-he"); will match
     * "-hello", "-help", "-her", etc
     */
    public boolean containsSwitchStart(String switch_str) {
        for (String arg : args) {
            if (arg.startsWith(switch_str)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a list of all switches on the command line that start with the
     * given string.  eg. command_line.allSwitchesStartingWith("-D"); will
     * return matches for switches "-Dname=toby", "-Dog", "-Dvalue=over", etc.
     */
    public String[] allSwitchesStartingWith(String switch_str) {
        Vector list = new Vector();
        for (String arg : args) {
            if (arg.startsWith(switch_str)) {
                list.addElement(arg);
            }
        }
        return (String[]) list.toArray(new String[list.size()]);
    }

    /**
     * Returns the contents of a switch variable if the switch is found on the
     * command line.  A switch variable is of the form '-[switch] [variable]'.
     * eg. 'command.exe -url "http://www.pony.com/database/"'.
     * <p>
     * Returns 'null' if the argument was not found.
     */
    public String switchArgument(String switch_str) {
        for (int i = 0; i < args.length - 1; ++i) {
            if (args[i].equals(switch_str)) {
                return args[i + 1];
            }
        }
        return null;
    }

    /**
     * Returns the contents of a switch variable if the switch is found on the
     * command line.  A switch variable is of the form '-[switch] [variable]'.
     * eg. 'command.exe -url "http://www.pony.com/database/"'.
     * <p>
     * Returns def if the argument was not found.
     */
    public String switchArgument(String switch_str, String def) {
        String arg = switchArgument(switch_str);
        if (arg == null) {
            return def;
        }
        return arg;
    }

    /**
     * Returns the contents of a set of arguments found after a switch on the
     * command line.  For example, switchArguments("-create", 3) would try and
     * find the '-create' switch and return the first 3 arguments after it if
     * it can.
     * <p>
     * Returns null if no match is found.
     */
    public String[] switchArguments(String switch_str, int arg_count) {
        for (int i = 0; i < args.length - 1; ++i) {
            if (args[i].equals(switch_str)) {
                if (i + arg_count < args.length) {
                    String[] ret_list = new String[arg_count];
                    for (int n = 0; n < arg_count; ++n) {
                        ret_list[n] = args[i + n + 1];
                    }
                    return ret_list;
                }
            }
        }
        return null;
    }


}
