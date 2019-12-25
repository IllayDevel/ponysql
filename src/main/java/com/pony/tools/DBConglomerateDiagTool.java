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

import com.pony.database.*;
import com.pony.database.control.*;
import com.pony.util.CommandLine;
import com.pony.debug.Lvl;

import javax.swing.*;
import javax.swing.event.*;
import javax.swing.table.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;

/**
 * An interactive tool for diagnosing the contents of a TableDataConglomerate
 * object.
 *
 * @author Tobias Downer
 */

public class DBConglomerateDiagTool {

    private static void diagnose(String path, String name) {
        try {
            TransactionSystem system = new TransactionSystem();
            DefaultDBConfig config = new DefaultDBConfig();
            config.setDatabasePath(path);
            config.setLogPath("");
            config.setMinimumDebugLevel(50000);
            config.setReadOnly(true);
            system.setDebugOutput(new StringWriter());
            system.init(config);
            system.setDebugLevel(Lvl.INFORMATION);

            final TableDataConglomerate conglomerate =
                    new TableDataConglomerate(system, system.storeSystem());
            // Open it.
            conglomerate.open(name); //false);
            // Open the main window
            final JFrame frame = new JFrame("Pony SQL Database Diagnostic Tool");
            frame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
            Container c = frame.getContentPane();
            c.setLayout(new BorderLayout());
            c.add(new ConglomerateViewPane(conglomerate), BorderLayout.CENTER);

            frame.addWindowListener(new WindowAdapter() {
                public void windowClosing(WindowEvent evt) {
                    try {
                        frame.dispose();
                        conglomerate.close();
                        System.exit(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });

            frame.pack();
            frame.setVisible(true);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Prints the syntax.
     */
    private static void printSyntax() {
        System.out.println("DBConglomerateDiagTool -path [data directory] " +
                "[-name [database name]]");
    }

    /**
     * Application start point.
     */
    public static void main(String[] args) {
        CommandLine cl = new CommandLine(args);

        String path = cl.switchArgument("-path");
        String name = cl.switchArgument("-name", "DefaultDatabase");

        if (path == null) {
            printSyntax();
            System.out.println("Error: -path not found on command line.");
            System.exit(-1);
        }

        // Start the conversion.
        diagnose(path, name);

    }

    // ---------- Inner classes ----------

}


/**
 * The pane the shows the contents of a conglomerate.
 */
class ConglomerateViewPane extends JRootPane {

    /**
     * The conglomerate.
     */
    private final TableDataConglomerate conglomerate;

    /**
     * The current selected table.
     */
    private RawDiagnosticTable current_table;

//  /**
//   * The JDesktopPane.
//   */
//  private JDesktopPane desktop;

    /**
     * The view of the store.
     */
    private final StoreViewPane view_pane;

    /**
     * Constructor.
     */
    public ConglomerateViewPane(TableDataConglomerate conglom) {
        setPreferredSize(new Dimension(750, 550));

        this.conglomerate = conglom;

        JMenu info = new JMenu("Info");
        Action average_row_count_action = new AbstractAction("Table Statistics") {

            public void actionPerformed(ActionEvent evt) {
                if (current_table == null || current_table.physicalRecordCount() < 10) {
                    return;
                }
                int row_count = current_table.physicalRecordCount();
                int[] store = new int[row_count];
                long total_size = 0;

                for (int i = 0; i < row_count; ++i) {
                    int record_size = current_table.recordSize(i);
                    store[i] = record_size;
                    total_size += record_size;
                }

                int avg = (int) (total_size / row_count);
                System.out.println("Average row size: " + avg);

                double best_score = 100000000000.0;
                int best_ss = 45;
                int best_size = Integer.MAX_VALUE;

                for (int ss = 19; ss < avg + 128; ++ss) {
                    int total_sectors = 0;
                    for (int n = 0; n < store.length; ++n) {
                        int sz = store[n];
                        total_sectors += (sz / (ss + 1)) + 1;
                    }

                    int file_size = ((ss + 5) * total_sectors);
                    double average_sec = (double) total_sectors / row_count;
                    double score = file_size * (((average_sec - 1.0) / 20.0) + 1.0);

                    System.out.println(" (" + score + " : " + file_size +
                            " : " + ss + " : " + average_sec + ") ");
                    if (average_sec < 2.8 && score < best_score) {
                        best_score = score;
                        best_size = file_size;
                        best_ss = ss;
                    }
                }

                System.out.println("Best sector size: " + best_ss +
                        " Best file size: " + best_size);


            }

        };
        info.add(average_row_count_action);

        JMenuBar menubar = new JMenuBar();
        menubar.add(info);

        setJMenuBar(menubar);

        Container c = getContentPane();
        c.setLayout(new BorderLayout());

        // The view pane.
        view_pane = new StoreViewPane();

        // Add the table list view to the desktop.
        final String[] str_t_list = conglomerate.getAllTableFileNames();
        final JList table_list = new JList(str_t_list);
        table_list.addListSelectionListener(new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent evt) {
                int index = table_list.getSelectedIndex();
                current_table = conglomerate.getDiagnosticTable(str_t_list[index]);
                view_pane.setDiagnosticModel(current_table);
            }
        });
        JScrollPane scrolly_table_list = new JScrollPane(table_list);

        // The split pane.
        JSplitPane split_pane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        split_pane.setLeftComponent(scrolly_table_list);
        split_pane.setRightComponent(view_pane);

        c.add(split_pane, BorderLayout.CENTER);

//    // The frame that shows the contents of the store.
//    JInternalFrame frame = new JInternalFrame("Store content", true);
//    frame.setPreferredSize(new Dimension(570, 520));
//    c = frame.getContentPane();
//    c.add(view_pane);
//    desktop.add(frame);
//    frame.setLocation(new Point(170, 10));
//    frame.pack();
//    frame.setVisible(true);
//
//
//    frame = new JInternalFrame("Store List", true);
//    frame.setPreferredSize(new Dimension(150, 520));
//    c = frame.getContentPane();
//    c.add(new JScrollPane(table_list));
//    desktop.add(frame);
//    frame.setLocation(new Point(10, 10));
//    frame.pack();
//    frame.setVisible(true);


    }

    // ---------- Inner classes ----------

    /**
     * The view of the content of a store.
     */
    private static class StoreViewPane extends JPanel {

        /**
         * The JTable that contains the information.
         */
        private final JTable table;


        public StoreViewPane() {
            setLayout(new BorderLayout());

            table = new JTable();
            add(new JScrollPane(table));
        }

        /**
         * Sets this store with the given RaDiagnosticTable model.
         */
        void setDiagnosticModel(RawDiagnosticTable model) {
            table.setModel(new DTableModel(model));
        }

    }

    /**
     * A TableModel for displaying the contents of a RawDiagnosticTable.
     */
    private static class DTableModel extends AbstractTableModel {

        private final RawDiagnosticTable diag_table;

        public DTableModel(RawDiagnosticTable diag_table) {
            this.diag_table = diag_table;
        }

        public int getRowCount() {
            return diag_table.physicalRecordCount();
        }

        public int getColumnCount() {
            return diag_table.getDataTableDef().columnCount() + 2;
        }

        public String getColumnName(int col) {
            if (col == 0) {
                return "Num";
            } else if (col == 1) {
                return "State";
            } else {
                return diag_table.getDataTableDef().columnAt(col - 2).getName();
            }
        }

        public Object getValueAt(int row, int col) {
            try {

                if (col == 0) {
                    return new Integer(row);
                } else if (col == 1) {
                    int type = diag_table.recordState(row);
                    if (type == diag_table.UNCOMMITTED) {
                        return "U";
                    } else if (type == diag_table.COMMITTED_ADDED) {
                        return "CA";
                    } else if (type == diag_table.COMMITTED_REMOVED) {
                        return "CR";
                    } else if (type == diag_table.DELETED) {
                        return "D";
                    } else {
                        return "#ERROR#";
                    }
                } else {
                    return diag_table.getCellContents(col - 2, row).getObject();
                }
            } catch (Throwable e) {
                return "#ERROR:" + e.getMessage() + "#";
            }
        }
    }

}


