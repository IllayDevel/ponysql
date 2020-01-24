/*
 * Pony SQL Database ( http://i-devel.ru )
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

package com.pony.debug;

import java.io.PrintWriter;
import java.io.Writer;

/**
 * A default implementation of DebugLogger that logs messages to
 * a PrintWriter object.
 * <p>
 * This implementation allows for filtering of log messages of particular
 * depth.  So for example, only message above or equal to level ALERT are
 * shown.
 *
 * @author Tobias Downer
 */

public class DefaultDebugLogger implements DebugLogger {

    /**
     * Set this to true if all alerts to messages are to be output to System.out.
     * The purpose of this flag is to aid debugging.
     */
    private static final boolean PRINT_ALERT_TO_MESSAGES = false;


    /**
     * The debug lock object.
     */
    private final Object debug_lock = new Object();

    /**
     * The PrintWriter for the system output stream.
     */
    static final PrintWriter SYSTEM_OUT = new PrintWriter(System.out, true);

    /**
     * The PrintWriter for the system error stream.
     */
    static final PrintWriter SYSTEM_ERR = new PrintWriter(System.err, true);


    /**
     * This variable specifies the level of debugging information that is
     * output.  Any debugging output above this level is output.
     */
    private int debug_level = 0;

    /**
     * The print stream where the debugging information is output to.
     */
    private PrintWriter out = SYSTEM_ERR;

    /**
     * The print stream where the error information is output to.
     */
    private PrintWriter err = SYSTEM_ERR;


    /**
     * Internal method that writes out the given information on the output
     * stream provided.
     */
    private void internalWrite(PrintWriter out,
                               int level, String class_string, String message) {
        synchronized (out) {
            if (level < MESSAGE) {
                out.print("> ");
                out.print(class_string);
                out.print(" ( lvl: ");
                out.print(level);
                out.print(" )\n  ");
            } else {
                out.print("% ");
            }
            out.println(message);
            out.flush();
        }
    }

    /**
     * Sets up the OutputStream to which the debug information is to be output
     * to.
     */
    public final void setOutput(Writer out) {
        this.out = new PrintWriter(out, false);
    }

    /**
     * Sets the debug level that's to be output to the stream.  Set to 255 to
     * stop all output to the stream.
     */
    public final void setDebugLevel(int level) {
        debug_level = level;
    }

    /**
     * Sets up the system so that the debug messenger will intercept event
     * dispatch errors and output the event to the debug stream.
     */
    public final void listenToEventDispatcher() {
        // This is only possible in versions of Java post 1.1
//#IFDEF(NO_1.1)
        // According to the EventDispatchThread documentation, this is just a
        // temporary hack until a proper API has been defined.
        System.setProperty("sun.awt.exception.handler",
                "com.pony.debug.DispatchNotify");
//#ENDIF
    }


    // ---------- Implemented from DebugLogger ----------

    public final boolean isInterestedIn(int level) {
        return (level >= debug_level);
    }

    public final void write(int level, Object ob, String message) {
        write(level, ob.getClass().getName(), message);
    }

    public final void write(int level, Class cla, String message) {
        write(level, cla.getName(), message);
    }

    public final void write(int level, String class_string, String message) {
        if (isInterestedIn(level)) {

            if (level >= ERROR && level < MESSAGE) {
                internalWrite(SYSTEM_ERR, level, class_string, message);
            } else if (PRINT_ALERT_TO_MESSAGES) {
                if (out != SYSTEM_ERR && level >= ALERT) { // && level < MESSAGE) {
                    internalWrite(SYSTEM_ERR, level, class_string, message);
                }
            }

            internalWrite(out, level, class_string, message);
        }

    }

    private void writeTime() {
        synchronized (out) {
            out.print("[ TIME: ");
            out.print(new java.util.Date(System.currentTimeMillis()));
            out.println(" ]");
            out.flush();
        }
    }

    public final void writeException(Throwable e) {
        writeException(ERROR, e);
    }

    public synchronized final void writeException(int level, Throwable e) {

        if (level >= ERROR) {
            synchronized (SYSTEM_ERR) {
                SYSTEM_ERR.print("[com.pony.debug.Debug - Exception thrown: '");
                SYSTEM_ERR.print(e.getMessage());
                SYSTEM_ERR.println("']");
                e.printStackTrace(SYSTEM_ERR);
            }
        }

        if (isInterestedIn(level)) {
            synchronized (out) {
                writeTime();
                out.print("% ");
                e.printStackTrace(out);
                out.flush();
            }
        }

    }

}
