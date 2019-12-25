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

package com.pony.database;

import com.pony.store.*;
import com.pony.debug.Lvl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * An implementation of StoreSystem that manages persistant data through the
 * native file system.  Each store is represented by a ScatteringFileStore
 * object against the current path.  This implementation is compatible with
 * versions of the database from 0.94 onwards.
 *
 * @author Tobias Downer
 */

class V1FileStoreSystem implements StoreSystem {

    /**
     * The name of the file extention of the file lock on this conglomerate.
     */
    private static final String FLOCK_EXT = ".lock";

    /**
     * The TransactionSystem that contains the various configuration options for
     * the database.
     */
    private final TransactionSystem system;

    /**
     * The path in the filesystem where the data files are located.
     */
    private final File path;

    /**
     * True if the stores are read-only.
     */
    private final boolean read_only;

    /**
     * The lock file.
     */
    private FileOutputStream lock_file;

    /**
     * Constructor.
     */
    public V1FileStoreSystem(TransactionSystem system, File path,
                             boolean read_only) {
        this.system = system;
        this.path = path;
        this.read_only = read_only;
        // If the database path doesn't exist, create it now,
        if (!read_only && !path.exists()) {
            path.mkdirs();
        }
    }

    /**
     * Creates the JournalledFileStore object for this table.
     */
    private JournalledFileStore createFileStore(String file_name)
            throws IOException {
        LoggingBufferManager buffer_manager = system.getBufferManager();
        return new JournalledFileStore(file_name, buffer_manager, read_only);
    }

    // ---------- Implemented from StoreSystem ----------

    public boolean storeExists(String name) {
        try {
            JournalledFileStore store = createFileStore(name);
            return store.exists();
        } catch (IOException e) {
            system.Debug().writeException(e);
            throw new RuntimeException("IO Error: " + e.getMessage());
        }
    }

    public Store createStore(String name) {
        LoggingBufferManager buffer_manager = system.getBufferManager();
        if (read_only) {
            throw new RuntimeException(
                    "Can not create store because system is read-only.");
        }
        try {
            buffer_manager.lockForWrite();

            JournalledFileStore store = createFileStore(name);
            if (!store.exists()) {
                store.open();
                return store;
            } else {
                throw new RuntimeException("Can not create - store with name " + name +
                        " already exists.");
            }
        } catch (IOException e) {
            system.Debug().writeException(e);
            throw new RuntimeException("IO Error: " + e.getMessage());
        } catch (InterruptedException e) {
            throw new Error("Interrupted: " + e.getMessage());
        } finally {
            buffer_manager.unlockForWrite();
        }

    }

    public Store openStore(String name) {
        LoggingBufferManager buffer_manager = system.getBufferManager();
        try {
            buffer_manager.lockForWrite();

            JournalledFileStore store = createFileStore(name);
            if (store.exists()) {
                store.open();
                return store;
            } else {
                throw new RuntimeException("Can not open - store with name " + name +
                        " does not exist.");
            }
        } catch (IOException e) {
            system.Debug().writeException(e);
            throw new RuntimeException("IO Error: " + e.getMessage());
        } catch (InterruptedException e) {
            throw new Error("Interrupted: " + e.getMessage());
        } finally {
            buffer_manager.unlockForWrite();
        }

    }

    public boolean closeStore(Store store) {
        LoggingBufferManager buffer_manager = system.getBufferManager();
        try {
            buffer_manager.lockForWrite();

            ((JournalledFileStore) store).close();
            return true;
        } catch (IOException e) {
            system.Debug().writeException(e);
            throw new RuntimeException("IO Error: " + e.getMessage());
        } catch (InterruptedException e) {
            throw new Error("Interrupted: " + e.getMessage());
        } finally {
            buffer_manager.unlockForWrite();
        }

    }

    public boolean deleteStore(Store store) {
        LoggingBufferManager buffer_manager = system.getBufferManager();
        try {
            buffer_manager.lockForWrite();

            return ((JournalledFileStore) store).delete();
        } catch (IOException e) {
            system.Debug().writeException(e);
            throw new RuntimeException("IO Error: " + e.getMessage());
        } catch (InterruptedException e) {
            throw new Error("Interrupted: " + e.getMessage());
        } finally {
            buffer_manager.unlockForWrite();
        }

    }

    public void setCheckPoint() {
        try {
            LoggingBufferManager buffer_manager = system.getBufferManager();
            buffer_manager.setCheckPoint(false);
        } catch (IOException e) {
            system.Debug().writeException(e);
            throw new RuntimeException("IO Error: " + e.getMessage());
        } catch (InterruptedException e) {
            system.Debug().writeException(e);
            throw new RuntimeException("Interrupted Error: " + e.getMessage());
        }
    }

    public void lock(String name) throws IOException {
        File flock_fn = new File(path, name + FLOCK_EXT);
        if (flock_fn.exists()) {
            // Okay, the file lock exists.  This means either an extremely bad
            // crash or there is another database locked on the files.  If we can
            // delete the lock then we can go on.
            system.Debug().write(Lvl.WARNING, this,
                    "File lock file exists: " + flock_fn);
            boolean deleted = false;
            deleted = flock_fn.delete();
            if (!deleted) {
                // If we couldn't delete, then most likely database being used.
                System.err.println("\n" +
                        "I couldn't delete the file lock for Database '" + name + "'.\n" +
                        "This most likely means the database is open and being used by\n" +
                        "another process.\n" +
                        "The lock file is: " + flock_fn + "\n\n");
                throw new IOException("Couldn't delete conglomerate file lock.");
            }
        }
//#IFDEF(NO_1.1)
        // Atomically create the file,
        flock_fn.createNewFile();
        // Set it to delete on normal exit of the JVM.
        flock_fn.deleteOnExit();
//#ENDIF
        // Open up a stream and lock it in the OS
        lock_file = new FileOutputStream(flock_fn);
    }

    public void unlock(String name) throws IOException {
        // Close and delete the lock file.
        if (lock_file != null) {
            lock_file.close();
        }
        // Try and delete it
        File flock_fn = new File(path, name + FLOCK_EXT);
        flock_fn.delete();
    }

}

