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

package com.pony.jfccontrols;

import java.awt.*;
import javax.swing.SwingUtilities;

/**
 * Helper class for providing blocking behaviour on the AWT/Swing event
 * dispatcher thread without freezing up the user interface.  While the call
 * to 'block' will block with respect to the callee, events will still be
 * serviced from within the 'block' method.
 * <p>
 * I consider this a mild hack.  This class may be incompatible with future
 * versions of Java if the AWT event mechanism is altered.  It may also not
 * work happily with non-Sun based implementations of Java.
 *
 * @author Tobias Downer
 */

public class SwingBlockUtil {

    /**
     * The state we are currently in.
     */
    private int block_state = 0;


    /**
     * Utility that blocks the Swing EventDispatchThread, and then emulates the
     * inner loop of the dispatcher thread itself.  This allows for repaint and
     * button events to be processed.  When the block has finished, this method
     * will return and return control to the originating event dispatcher.
     */
    public void block() {

        synchronized (this) {
            if (!SwingUtilities.isEventDispatchThread()) {
                throw new Error("Not on the event dispatcher.");
            }
            if (block_state != 0) {
                // This situation would occur when a component generates an event (such
                // as the user pressing a button) that performs a query.  Therefore
                // there are multi-levels of queries being executed.
                throw new Error("Can't nest queries.");
            }

            block_state = 1;
        }

        EventQueue theQueue = eventQueue();
        while (isBlocked()) {
            try {
                // This is essentially the body of EventDispatchThread
                AWTEvent event = theQueue.getNextEvent();
                Object src = event.getSource();
                // can't call theQueue.dispatchEvent, so I pasted its body here
                if (event instanceof ActiveEvent) {
                    ((ActiveEvent) event).dispatch();
                } else if (src instanceof Component) {
                    ((Component) src).dispatchEvent(event);
                } else if (src instanceof MenuComponent) {
                    ((MenuComponent) src).dispatchEvent(event);
                } else {
                    System.err.println("unable to dispatch event: " + event);
                }
            } catch (Throwable e) {
                // Any exceptions thrown here are logged, but we don't break the loop.
                System.err.println("Exception thrown during block util dispatching:");
                e.printStackTrace(System.err);
            }
        }

        block_state = 0;

    }

    /**
     * Unblocks any call to the 'block' method.  This method can safely be
     * executed from any thread (even the Swing dispatcher thread).
     */
    public void unblock() {
        // Execute the runnable on the event dispatcher,
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                if (block_state == 1) {
                    block_state = 2;
                }
            }
        });
    }

    /**
     * Returns true if the event dispatcher is blocked.
     */
    private boolean isBlocked() {
        return block_state <= 1;
    }

    /**
     * Returns the current system EventQueue.
     */
    private static EventQueue eventQueue() {
        return Toolkit.getDefaultToolkit().getSystemEventQueue();
    }

}
