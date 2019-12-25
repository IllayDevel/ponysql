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

import com.pony.jfccontrols.ResultSetTableModel;
import com.pony.jfccontrols.QueryAgent;
import com.pony.jfccontrols.Query;
import com.pony.util.CommandLine;

import java.sql.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.border.*;

/**
 * A graphical interactive SQL query tool that allows for queries to be
 * executed to a JDBC driver.
 *
 * @author Tobias Downer
 */

public class JDBCQueryTool extends JComponent {

    /**
     * The agent used to make queries on the JDBC connection.
     */
    private QueryAgent query_agent;

    /**
     * The JTextArea where the query is entered.
     */
    private JTextArea query_text_area;

    /**
     * The JTable where the query result is printed.
     */
    private JTable result_table;

    /**
     * The ResultSetTableModel for the table model that contains our result set.
     */
    private ResultSetTableModel table_model;

    /**
     * The JLabel status bar at the bottom of the window.
     */
    private JLabel status_text;

    /**
     * Set to true if the table is auto resize (default).
     */
    protected JCheckBoxMenuItem auto_resize_result_table;

    /**
     * Total number of rows in the result.
     */
    private int total_row_count;

    /**
     * The time it took to execute the query in milliseconds.
     */
    private int query_time = -1;

    /**
     * Constructs the JComponent.
     */
    public JDBCQueryTool(QueryAgent in_query_agent) {
        this.query_agent = in_query_agent;

        // --- Layout ---

        // Toggle auto result columns.

        auto_resize_result_table = new JCheckBoxMenuItem("Auto Resize Columns");
        auto_resize_result_table.setSelected(true);
        auto_resize_result_table.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                if (auto_resize_result_table.isSelected()) {
                    result_table.setAutoResizeMode(
                            JTable.AUTO_RESIZE_SUBSEQUENT_COLUMNS);
                } else {
                    result_table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
                }
            }
        });

        // Main window

        setLayout(new BorderLayout());
        setBorder(new EmptyBorder(2, 2, 2, 2));

        JPanel query_area = new JPanel();
        query_area.setLayout(new BorderLayout());

        // Mono-space font.
        Font mono_font = new Font("MonoSpaced", Font.PLAIN, 12);

        // The query text area
        query_text_area = new JTextArea(7, 80);
        query_text_area.setFont(mono_font);

        // The execute and cancel query button
        final JButton execute = new JButton("Run Query");
        final JButton stop = new JButton("Stop Query");
        stop.setEnabled(false);
        JPanel button_bar = new JPanel();
        button_bar.setLayout(new FlowLayout());
        button_bar.add(execute);
        button_bar.add(stop);

        // The query area
        query_area.add(new JScrollPane(query_text_area), BorderLayout.CENTER);
        query_area.add(button_bar, BorderLayout.SOUTH);

        table_model = new ResultSetTableModel();
        table_model.setPreserveTableStructure(true);
        result_table = new JTable(table_model);
        JScrollPane scrolly_result_table = new JScrollPane(result_table);
        scrolly_result_table.setPreferredSize(new Dimension(650, 450));

        // The status bar.
        status_text = new JLabel("  ");
        status_text.setFont(mono_font);
        status_text.setBorder(new BevelBorder(BevelBorder.LOWERED));

        // Make the split pane
        JSplitPane split_pane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        split_pane.setTopComponent(query_area);
        split_pane.setBottomComponent(scrolly_result_table);

        add(split_pane, BorderLayout.CENTER);

        add(status_text, BorderLayout.SOUTH);

        // --- Actions ---

        execute.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                try {

                    stop.setEnabled(true);
                    execute.setEnabled(false);

                    Query query = new Query(query_text_area.getText());
                    long time_start = System.currentTimeMillis();
                    ResultSet result_set = query_agent.executeQuery(query);
                    query_time = (int) (System.currentTimeMillis() - time_start);

                    // Scroll the result view to the top and update the model
                    result_table.scrollRectToVisible(new Rectangle(0, 0, 1, 1));
                    table_model.updateResultSet(result_set);

                    total_row_count = 0;
                    if (result_set.last()) {
                        total_row_count = result_set.getRow();
                    }
                    updateStatus();

                } catch (SQLException e) {
                    JOptionPane.showMessageDialog(JDBCQueryTool.this,
                            e.getMessage(), "SQL Error", JOptionPane.ERROR_MESSAGE);
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    System.err.println("Query cancelled.");
                }
                stop.setEnabled(false);
                execute.setEnabled(true);
            }
        });

        stop.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                query_agent.cancelQuery();
            }
        });


    }

    /**
     * Constructs the JComponent.
     */
    public JDBCQueryTool(Connection connection) {
        this(new QueryAgent(connection));
    }

    /**
     * Updates the status bar.
     */
    private void updateStatus() {
        StringBuffer buf = new StringBuffer();
        buf.append("Query Time: ");
        buf.append((double) query_time / 1000.0);
        buf.append(" seconds.  Row Count: ");
        buf.append(total_row_count);
        status_text.setText(new String(buf));
    }

    // ----- Static methods -----

    /**
     * The number of query windows we have open.
     */
    private static int query_window_count = 0;

    /**
     * Creates a new JDBC Query Tool window.  Use this method to open a new
     * query window in a JFrame to the database.
     *
     * @param connection the connection to the database
     * @param close_connection_on_close if true the JDBC Connection is closed
     *   when the last query tool window is closed.
     * @param system_exit_on_close if true the JVM will shut down when the last
     *   query tool window is closed.
     */
    public static void createNewWindow(final Connection connection,
                                       final boolean close_connection_on_close,
                                       final boolean system_exit_on_close) {
        // Increment the count of windows open.
        ++query_window_count;

        // The QueryAgent for this frame.
        final QueryAgent query_agent = new QueryAgent(connection);

        // Make the window,
        final JFrame frame = new JFrame("Pony JDBC Query Tool");

        // The action to close this window,
        final Action close_action = new AbstractAction("Exit") {
            public void actionPerformed(ActionEvent evt) {
                frame.dispose();
                // Decrement the count of windows open.
                --query_window_count;
                // If last window closed then close the connection and exit back to the
                // system.
                if (query_window_count == 0) {
                    if (close_connection_on_close) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            System.err.println("SQL Exception on close: " +
                                    e.getMessage());
                        }
                    }
                    if (system_exit_on_close) {
                        System.exit(0);
                    }
                }
            }
        };

        // The action to clone this window
        final Action clone_action = new AbstractAction("New Window") {
            public void actionPerformed(ActionEvent evt) {
                createNewWindow(connection,
                        close_connection_on_close, system_exit_on_close);
            }
        };

        // --- The layout ---

        Container c = frame.getContentPane();
        c.setLayout(new BorderLayout());
        JDBCQueryTool query_tool = new JDBCQueryTool(query_agent);
        c.add(query_tool, BorderLayout.CENTER);

        // Set the menu bar for the window.
        JMenuBar menu_bar = new JMenuBar();
        JMenu file = new JMenu("File");
        file.add(clone_action);
        file.addSeparator();
        file.add(close_action);
        menu_bar.add(file);

        JMenu options = new JMenu("Options");
        options.add(query_tool.auto_resize_result_table);
        menu_bar.add(options);

        frame.setJMenuBar(menu_bar);

        // Pack and show the window.
        frame.pack();
        frame.show();

        // If frame is closed then perform the close action.
        frame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent evt) {
                close_action.actionPerformed(null);
            }
        });

    }

    /**
     * Prints the syntax to System.out.
     */
    private static void printSyntax() {
        System.out.println("JDBCQueryTool [-jdbc JDBC_Driver_Class] [-url JDBC_URL] -u username -p password");
    }

    /**
     * Application start point.
     */
    public static void main(String[] args) {
        CommandLine cl = new CommandLine(args);

        String driver = cl.switchArgument("-jdbc", "com.pony.JDBCDriver");
        String url = cl.switchArgument("-url", "jdbc:pony:");
        String username = cl.switchArgument("-u");
        String password = cl.switchArgument("-p");

        if (username == null) {
            System.out.println("Please provide a username");
            System.out.println();
            printSyntax();
        } else if (password == null) {
            System.out.println("Please provide a password");
            System.out.println();
            printSyntax();
        } else {

            try {
                System.out.println("Using JDBC Driver: " + driver);

                // Register the driver.
                Class.forName(driver).newInstance();

                // Make a connection to the server.
                final Connection connection =
                        DriverManager.getConnection(url, username, password);

                System.out.println("Connection established to: " + url);

                // Invoke the tool on the swing event dispatch thread.
                SwingUtilities.invokeLater(new Runnable() {
                    public void run() {
                        createNewWindow(connection, true, true);
                    }
                });

            } catch (ClassNotFoundException e) {
                System.out.println("JDBC Driver not found.");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

}
