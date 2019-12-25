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

package com.pony.tools;

import com.pony.util.CommandLine;
import com.pony.util.ResultOutputUtil;

import java.sql.*;
import java.io.*;
import java.util.Vector;
import java.util.Hashtable;
import java.util.Enumeration;

/**
 * A tool that reads an input SQL script and output information for the
 * result either to an output file or through System.out.  This is a useful
 * command line tool that can be used for regression testing and database
 * diagnostics (as well as for basic SQL scripting needs).
 * <p>
 * This class is able to output result sets in textual form.
 *
 * @author Tobias Downer
 */

public class JDBCScriptTool {

    /**
     * The Reader we are reading the script commands from.
     */
    private Reader in;

    /**
     * The Writer we are outputing the script results to.
     */
    private PrintWriter out;

    /**
     * Constructs the tool.
     */
    public JDBCScriptTool(Reader input, PrintWriter output) {
        this.in = input;
        this.out = output;
    }

    /**
     * Fetches the next query from the input.  Returns null if no more queries
     * available.
     */
    private String nextQuery() throws IOException {
        StringBuffer query = new StringBuffer();
        int c = in.read();
        boolean command = false;
        while (c != -1) {

            if (command == true ||
                    (c != ' ' && c != '\n' && c != '\r' && c != '\t')) {
                command = true;
            }

            query.append((char) c);
            if (c == ';') {
                return new String(query);
            }
            c = in.read();
        }
        if (command == false) {
            return null;
        }
        return new String(query);
    }

    /**
     * Evaluates the input script and outputs the result to the output stream.
     * Returns the list of Connections established.
     */
    public Connection[] evaluate(Connection connection, String url,
                                 String username, String password) throws IOException {
        // Read in the query.  The query is everything up to the ';' character
        // which denotes the end of an SQL command.
        // ISSUE: It currently does not recognise ';' if used inside a string...

        Statement statement;
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            out.println("SQL Error creating statement: " + e.getMessage());
            return new Connection[]{connection};
        }

        // A hash table of connections.
        Hashtable connections = new Hashtable();
        connections.put("default", connection);

        String query = nextQuery();
        while (query != null) {

            try {
                // Check it's not an internal command.
                String command =
                        query.substring(0, query.length() - 1).trim().toLowerCase();
                if (command.startsWith("switch to connection ")) {
                    String connection_name = command.substring(21);
                    Connection c = (Connection) connections.get(connection_name);
                    if (c == null) {
                        c = DriverManager.getConnection(url, username, password);
                        connections.put(connection_name, c);
                        out.println("Established Connection: " + connection_name);
                    }
                    statement = c.createStatement();
                    out.println("Switched to Connection: " + connection_name);
                    out.flush();
                } else if (command.startsWith("//")) {
                    out.println();
                    out.println(command);
                    out.flush();
                } else {

                    out.println();
                    out.print("> ");
                    out.println(query.trim());
                    ResultSet result_set = statement.executeQuery(query);
                    ResultOutputUtil.formatAsText(result_set, out);
                    out.flush();
                }
            } catch (SQLException e) {
                out.println("SQL Error running query (\n" + query + "\n)\nError: " +
                        e.getMessage());
                out.println("Error code: " + e.getErrorCode());
//        return;
            }
            out.flush();

            // Fetch the next query
            query = nextQuery();
        }

        Enumeration e = connections.elements();
        Vector v = new Vector();
        while (e.hasMoreElements()) {
            v.addElement(e.nextElement());
        }

        Connection[] arr = new Connection[v.size()];
        for (int i = 0; i < arr.length; ++i) {
            arr[i] = (Connection) v.elementAt(i);
        }
        return arr;

    }


    // ---------- Application methods ----------

    /**
     * Prints the syntax to System.out.
     */
    private static void printSyntax() {
        System.out.println(
                "JDBCScriptTool [-jdbc JDBC_Driver_Class] [-url JDBC_URL] \n" +
                        "               -u username -p password \n" +
                        "               [-in Input_SQL_File] [-out Output_Result_File] \n" +
                        "\n" +
                        "  If -in or -out are not specified then the tool uses System.in \n" +
                        "  and System.out respectively.\n");
    }

    /**
     * The application start point.
     */
    public static void main(String[] args) {
        CommandLine cl = new CommandLine(args);

        String driver = cl.switchArgument("-jdbc", "com.pony.JDBCDriver");
        String url = cl.switchArgument("-url", ":jdbc:pony:");
        String username = cl.switchArgument("-u");
        String password = cl.switchArgument("-p");
        final String input_file = cl.switchArgument("-in");
        final String output_file = cl.switchArgument("-out");

        if (username == null) {
            System.out.println("Please provide a username");
            System.out.println();
            printSyntax();
            System.exit(1);
        } else if (password == null) {
            System.out.println("Please provide a password");
            System.out.println();
            printSyntax();
            System.exit(1);
        }

        Reader reader = null;
        PrintWriter writer = null;

        try {
            if (input_file == null) {
                reader = new InputStreamReader(System.in);
            } else {
                reader = new BufferedReader(new FileReader(input_file));
            }

            if (output_file == null) {
                writer = new PrintWriter(
                        new BufferedWriter(new OutputStreamWriter(System.out)));
            } else {
                writer = new PrintWriter(
                        new BufferedWriter(new FileWriter(output_file)));
            }

        } catch (IOException e) {
            System.err.println("IO Error: " + e.getMessage());
            printSyntax();
            System.exit(1);
        }

        // Create the JDBCScriptTool object
        JDBCScriptTool tool = new JDBCScriptTool(reader, writer);

        // Establish the connection,
        Connection connection = null;
        try {
            writer.println("Using JDBC Driver: " + driver);

            // Register the driver.
            Class.forName(driver).newInstance();

            // Make a connection to the server.
            connection = DriverManager.getConnection(url, username, password);

            writer.println("Connection established to: " + url);
            writer.flush();

        } catch (ClassNotFoundException e) {
            writer.println("JDBC Driver not found.");
            printSyntax();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            printSyntax();
            System.exit(1);
        }

        try {

            if (input_file != null) {
                System.out.println("Script input from: " + input_file);
            }

            // Evaluate the script...
            Connection[] conl = tool.evaluate(connection, url, username, password);

            // Close the input/output
            reader.close();
            writer.println();
            writer.println(" --- FINISHED");
            writer.close();

//      try {
//        Thread.sleep(4000);
//      }
//      catch (InterruptedException e) {
//      }

            for (int i = 0; i < conl.length; ++i) {
                conl[i].close();
            }
//      connection.close();

            if (output_file != null) {
                System.out.println("Script output to: " + output_file);
            }

        } catch (IOException e) {
            System.err.println("IO Error: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        } catch (SQLException e) {
            System.err.println("SQL Error: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }

    }


}
