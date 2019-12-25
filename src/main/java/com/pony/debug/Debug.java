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

package com.pony.debug;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Writer;

/**
 * This is a static class that should be used to output debugging information.
 * Since all debug messages go through this class, we can easily turn the
 * messages on and off, or specify output of different levels of debugging
 * information.  We can also filter out the debugging information and output
 * it to different output streams.
 *
 * @author Tobias Downer
 * @deprecated use DebugLogger implementations instead.
 */

public final class Debug {

    /**
     * Set this to true if all alerts to messages are to be output to System.out.
     * The purpose of this flag is to aid debugging.
     */
    private static final boolean PRINT_ALERT_TO_MESSAGES = false;

    /**
     * Set this to true to output all exceptions to System.err.
     */
    private static final boolean EXCEPTIONS_TO_ERR = false;


    /**
     * Some sample debug levels.
     */
    public final static int INFORMATION = 10;    // General processing 'noise'
    public final static int WARNING = 20;    // A message of some importance
    public final static int ALERT = 30;    // Crackers, etc
    public final static int ERROR = 40;    // Errors, exceptions
    public final static int MESSAGE = 10000; // Always printed messages
    // (not error's however)

    /**
     * The debug lock object.
     */
    private static final Object debug_lock = new Object();

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
    static int debug_level = 0;

    /**
     * The print stream where the debugging information is output to.
     */
    static PrintWriter out = SYSTEM_ERR;

    /**
     * The print stream where the error information is output to.
     */
    static PrintWriter err = SYSTEM_ERR;

    /**
     * Internal method that writes out the given information on the output
     * stream provided.
     */
    private static void internalWrite(PrintWriter out,
                                      int level, String class_string, String message) {
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

    /**
     * Sets up the OutputStream to which the debug information is to be output
     * to.
     */
    public static void setOutput(Writer out) {
        Debug.out = new PrintWriter(out, false);
    }

    /**
     * Sets the debug level that's to be output to the stream.  Set to 255 to
     * stop all output to the stream.
     */
    public static void setDebugLevel(int level) {
        debug_level = level;
    }

    /**
     * Sets up the system so that the debug messenger will intercept event
     * dispatch errors and output the event to the debug stream.
     */
    public static void listenToEventDispatcher() {
        // This is only possible in versions of Java post 1.1
//#IFDEF(NO_1.1)
        // According to the EventDispatchThread documentation, this is just a
        // temporary hack until a proper API has been defined.
        System.setProperty("sun.awt.exception.handler",
                "com.pony.debug.DispatchNotify");
//#ENDIF
    }


    /**
     * Queries the current debug level.  Returns true if the debug listener is
     * interested in debug information of this given level.  This can be used to
     * speed up certain complex debug displaying operations where the debug
     * listener isn't interested in the information be presented.
     */
    public static boolean isInterestedIn(int level) {
        return (level >= debug_level);
    }

    /**
     * This writes the given debugging string.  It filters out any messages that
     * are below the 'debug_level' variable.  The 'object' variable specifies
     * the object that made the call.  'level' must be between 0 and 255.  A
     * message of 'level' 255 will always print.
     */
    public static void write(int level, Object ob, String message) {
        write(level, ob.getClass().getName(), message);
    }

    public static void write(int level, Class cla, String message) {
        write(level, cla.getName(), message);
    }

    public static void write(int level, String class_string, String message) {
        if (isInterestedIn(level)) {

            synchronized (debug_lock) {
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
    }

    /**
     * @deprecated this is a legacy debug method.
     */
    public static void write(Object ob, String message) {
        write(5, ob, message);
    }

    /**
     * Writes out the time to the debug stream.
     */
    private static void writeTime() {
        out.print("[ TIME: ");
        out.print(new java.util.Date(System.currentTimeMillis()));
        out.println(" ]");
        out.flush();
    }

    /**
     * This writes the given Exception.  Exceptions are always output to the log
     * stream.
     */
    public static void writeException(Throwable e) {
        writeException(ERROR, e);
    }

    /**
     * This writes the given Exception but gives it a 'debug_level'.  This is
     * so we can write out a warning exception.
     */
    public static void writeException(int level, Throwable e) {

//    new Error().printStackTrace();

        synchronized (debug_lock) {
            if (level >= ERROR) {
                System.err.print("[com.pony.debug.Debug - Exception thrown: '");
                System.err.print(e.getMessage());
                System.err.println("']");
                e.printStackTrace(System.err);
            }

            if (isInterestedIn(level)) {
                writeTime();
                out.print("% ");
                e.printStackTrace(out);
                out.flush();
            }
        }
    }


}
