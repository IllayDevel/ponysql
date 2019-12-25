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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.InputStream;

/**
 * Reads a command block on the underlying stream that is constrained by
 * a length marker preceeding the command.  This can be used as a hack
 * work around for non-blocking IO because we know ahead of time how much data
 * makes up the next block of information over the stream.
 *
 * @author Tobias Downer
 */

public final class LengthMarkedBufferedInputStream extends FilterInputStream {

    /**
     * The initial buffer size of the internal input buffer.
     */
    private static int INITIAL_BUFFER_SIZE = 512;

    /**
     * The chained InputStream that is underneath this object.
     */
    private InputStream in;

    /**
     * The buffer that is used to read in whatever is on the stream.
     */
    private byte[] buf;

    /**
     * The number of valid bytes in the buffer.
     */
    private int count;

    /**
     * The area of the buffer that is marked as being an available command.
     * If it's -1 then there is no area marked.
     */
    private int marked_length;

    /**
     * The current index of the marked area that is being read.
     */
    private int marked_index;

    /**
     * The Constructor.
     */
    public LengthMarkedBufferedInputStream(InputStream in) {
        super(in);
        this.in = in;
        buf = new byte[INITIAL_BUFFER_SIZE];
        count = 0;
        marked_length = -1;
        marked_index = -1;
    }

    /**
     * Ensures that the buffer is large enough to store the given value.  If
     * it's not then it grows the buffer so it is big enough.
     */
    private void ensureCapacity(int new_size) {
        int old_size = buf.length;
        if (new_size > old_size) {
            int cap = (old_size * 3) / 2 + 1;
            if (cap < new_size)
                cap = new_size;
            byte[] old_buf = buf;
            buf = new byte[cap];
//      // Copy all the contents except the first 4 bytes (the size marker)
//      System.arraycopy(old_buf, 4, buf, 4, count - 4);
            System.arraycopy(old_buf, 0, buf, 0, count - 0);
        }
    }

    /**
     * Private method, it is called when the end of the marked length is reached.
     * It performs various maintenance operations to ensure the buffer
     * consistency is maintained.
     * Assumes we are calling from a synchronized method.
     */
    private void handleEndReached() {
//    System.out.println();
//    System.out.println("Shifting Buffer: ");
//    System.out.println(" Index: " + marked_index +
//                         ", Length: " + (count - marked_length));
        // Move anything from the end of the buffer to the start.
        System.arraycopy(buf, marked_index, buf, 0, count - marked_length);
        count -= marked_length;

        // Reset the state
        marked_length = -1;
        marked_index = -1;
    }

    // ---------- Overwritten from FilterInputStream ----------

    public synchronized int read() throws IOException {
        if (marked_index == -1) {
            throw new IOException("No mark has been read yet.");
        }
        if (marked_index >= marked_length) {
            String debug_msg = "Read over end of length marked buffer.  ";
            debug_msg += "(marked_index=" + marked_index;
            debug_msg += ",marked_length=" + marked_length + ")";
            debug_msg += ")";
            throw new IOException(debug_msg);
        }
        int n = buf[marked_index++] & 0x0FF;
        if (marked_index >= marked_length) {
            handleEndReached();
        }
        return n;
    }

    public synchronized int read(byte[] b, int off, int len) throws IOException {
        if (marked_index == -1) {
            throw new IOException("No mark has been read yet.");
        }
        int read_upto = marked_index + len;
        if (read_upto > marked_length) {
            String debug_msg = "Read over end of length marked buffer.  ";
            debug_msg += "(marked_index=" + marked_index;
            debug_msg += ",len=" + len;
            debug_msg += ",marked_length=" + marked_length + ")";
            throw new IOException(debug_msg);
        }
        System.arraycopy(buf, marked_index, b, off, len);
        marked_index = read_upto;
        if (marked_index >= marked_length) {
            handleEndReached();
        }
        return len;
    }

    public synchronized int available() throws IOException {
        // This method only returns a non 0 value if there is a complete command
        // waiting on the stream.
        if (marked_length >= 0) {
            return (marked_length - marked_index);
        }
        return 0;
    }

    public boolean markSupported() {
        return false;
    }

    // ---------- These methods aid in reading state from the stream ----------

    /**
     * Checks to see if there is a complete command waiting on the input stream.
     * Returns true if there is.  If this method returns true then it is safe
     * to go ahead and process a single command from this stream.
     * This will return true only once while there is a command pending until
     * that command is completely read in.
     * <p>
     * 'max_size' is the maximum number of bytes we are allowing before an
     * IOException is thrown.
     */
    public synchronized boolean pollForCommand(int max_size) throws IOException {
        if (marked_length == -1) {
            int available = in.available();
//      System.out.print(available);
//      System.out.print(", ");
            if (count > 0 || available > 0) {
                if ((count + available) > max_size) {
                    throw new IOException("Marked length is greater than max size ( " +
                            (count + available) + " > " + max_size + " )");
                }

                ensureCapacity(count + available);
                int read_in = in.read(buf, count, available);

//        System.out.println("-----");
//        for (int i = 0; i < available; ++i) {
//          System.out.print((char) buf[count + i] +
//                           "(" + (int) buf[count + i] + "),");
//        }
//        System.out.println("-----");


                if (read_in == -1) {
                    throw new EOFException();
                }
                count = count + read_in;

//        else if (read_in != available) {
//          throw new IOException("Read in size mismatch: " +
//                        "read_in: " + read_in + " available: " + available);
//        }

                // Check: Is a complete command available?
                if (count >= 4) {
                    int length_marker = (((buf[0] & 0x0FF) << 24) +
                            ((buf[1] & 0x0FF) << 16) +
                            ((buf[2] & 0x0FF) << 8) +
                            ((buf[3] & 0x0FF) << 0));
                    if (count >= length_marker + 4) {
                        // Yes, complete command available.
                        // mark this area up.
                        marked_length = length_marker + 4;
                        marked_index = 4;
//            System.out.println("Complete command available: ");
//            System.out.println("Length: " + marked_length +
//                               ", Index: " + marked_index);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Blocks until a complete command has been read in.
     */
    public synchronized void blockForCommand() throws IOException {
        while (true) {

            // Is there a command available?
            if (count >= 4) {
                int length_marker = (((buf[0] & 0x0FF) << 24) +
                        ((buf[1] & 0x0FF) << 16) +
                        ((buf[2] & 0x0FF) << 8) +
                        ((buf[3] & 0x0FF) << 0));
                if (count >= length_marker + 4) {
                    // Yes, complete command available.
                    // mark this area up.
                    marked_length = length_marker + 4;
                    marked_index = 4;
//          System.out.println("marked_length = " + marked_length);
//          System.out.println("marked_index = " + marked_index);
                    return;
                }
            }

            // If the buffer is full grow it larger.
            if (count >= buf.length) {
                ensureCapacity(count + INITIAL_BUFFER_SIZE);
            }
            // Read in a block of data, block if nothing there
            int read_in = in.read(buf, count, buf.length - count);
            if (read_in == -1) {
                throw new EOFException();
            }
            count += read_in;
        }
    }

}
