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

import java.io.*;

/**
 * A Writer that writes information to a log file that archives old log
 * entries when it goes above a certain size.
 *
 * @author Tobias Downer
 */

public class LogWriter extends Writer {

    /**
     * The log file.
     */
    private final File log_file;

    /**
     * The maximum size of the log before it is archived.
     */
    private final long max_size;

    /**
     * The number of backup archives of log files.
     */
    private final int archive_count;

    /**
     * Current size of the log file.
     */
    private long log_file_size;

    /**
     * The log file FileWriter.
     */
    private Writer out;

    /**
     * Constructs the log writer.  The 'base_name' is the name of log file.
     * 'max_size' is the maximum size the file can grow to before it is
     * copied to a log archive.
     */
    public LogWriter(File base_name, long max_size, int archive_count)
            throws IOException {

        if (archive_count < 1) {
            throw new IllegalArgumentException(
                    "'archive_count' must be 1 or greater.");
        }

        this.log_file = base_name;
        this.max_size = max_size;
        this.archive_count = archive_count;

        // Does the file exist?
        if (base_name.exists()) {
            log_file_size = base_name.length();
        } else {
            log_file_size = 0;
        }
        out = new BufferedWriter(new FileWriter(base_name.getPath(), true));

    }

    /**
     * Checks the size of the file, and if it has reached or exceeded the
     * maximum limit then archive the log.
     */
    private void checkLogSize() throws IOException {
        if (log_file_size > max_size) {
            // Flush to the log file,
            out.flush();
            // Close it,
            out.close();
            out = null;
            // Delete the top archive,
            File top = new File(log_file.getPath() + "." + archive_count);
            top.delete();
            // Rename backup archives,
            for (int i = archive_count - 1; i > 0; --i) {
                File source = new File(log_file.getPath() + "." + i);
                File dest = new File(log_file.getPath() + "." + (i + 1));
                source.renameTo(dest);
            }
            log_file.renameTo(new File(log_file.getPath() + ".1"));

            // Create the new empty log file,
            out = new BufferedWriter(new FileWriter(log_file.getPath(), true));
            log_file_size = 0;
        }
    }

    // ---------- Implemented from Writer ----------

    public synchronized void write(int c) throws IOException {
        out.write(c);
        ++log_file_size;
    }

    public synchronized void write(char[] cbuf, int off, int len)
            throws IOException {
        out.write(cbuf, off, len);
        log_file_size += len;
    }

    public synchronized void write(String str, int off, int len)
            throws IOException {
        out.write(str, off, len);
        log_file_size += len;
    }

    public synchronized void flush() throws IOException {
        out.flush();
        checkLogSize();
    }

    public synchronized void close() throws IOException {
        out.flush();
        out.close();
    }

}
