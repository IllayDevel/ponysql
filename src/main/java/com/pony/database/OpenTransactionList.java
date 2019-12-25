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

import java.util.ArrayList;

import com.pony.debug.Lvl;

/**
 * The list of all currently open transactions.  This is a thread safe
 * object that is shared between a TableDataConglomerate and its children
 * MasterDataTableSource objects.  It is used for maintaining a list of
 * transactions that are currently open in the system.  It also provides
 * various utility methods around the list.
 * <p>
 * This class is thread safe and can safely be accessed by multiple threads.
 * This is so threads accessing table source information as well as
 * conglomerate 'commit' stages can safely access this object.
 *
 * @author Tobias Downer
 */

final class OpenTransactionList {

    /**
     * True to enable transaction tracking.
     */
    private static final boolean TRACKING = false;

    /**
     * The system that this transaction list is part of.
     */
    private final TransactionSystem system;

    /**
     * The list of open transactions.
     * (Transaction).
     */
    private final ArrayList open_transactions;

    /**
     * A list of Error objects created when the transaction is added to the open
     * transactions list.
     */
    private ArrayList open_transaction_stacks;

    /**
     * The minimum commit id of the current list.
     */
    private long minimum_commit_id;

    /**
     * The maximum commit id of the current list.
     */
    private long maximum_commit_id;

    /**
     * Creates the list.
     */
    OpenTransactionList(TransactionSystem system) {
        this.system = system;
        open_transactions = new ArrayList();
        if (TRACKING) {
            open_transaction_stacks = new ArrayList();
        }
        minimum_commit_id = Long.MAX_VALUE;
        maximum_commit_id = 0;
    }

    /**
     * Adds a new open transaction to the list.  Transactions must be added
     * in order of commit_id.
     */
    synchronized void addTransaction(Transaction transaction) {
        long current_commit_id = transaction.getCommitID();
        if (current_commit_id >= maximum_commit_id) {
            open_transactions.add(transaction);
            if (TRACKING) {
                open_transaction_stacks.add(new Error());
            }
            system.stats().increment("OpenTransactionList.count");
            maximum_commit_id = current_commit_id;
        } else {
            throw new Error(
                    "Added a transaction with a lower than maximum commit_id");
        }
    }

    /**
     * Removes an open transaction from the list.
     */
    synchronized void removeTransaction(Transaction transaction) {

        int size = open_transactions.size();
        int i = open_transactions.indexOf(transaction);
        if (i == 0) {
            // First in list.
            if (i == size - 1) {
                // And last.
                minimum_commit_id = Integer.MAX_VALUE;
                maximum_commit_id = 0;
            } else {
                minimum_commit_id =
                        ((Transaction) open_transactions.get(i + 1)).getCommitID();
            }
        } else if (i == open_transactions.size() - 1) {
            // Last in list.
            maximum_commit_id =
                    ((Transaction) open_transactions.get(i - 1)).getCommitID();
        } else if (i == -1) {
            throw new Error("Unable to find transaction in the list.");
        }
        open_transactions.remove(i);
        if (TRACKING) {
            open_transaction_stacks.remove(i);
        }
        system.stats().decrement("OpenTransactionList.count");

        if (TRACKING) {
            system.Debug().write(Lvl.MESSAGE, this, "Stacks:");
            for (Object open_transaction_stack : open_transaction_stacks) {
                system.Debug().writeException(Lvl.MESSAGE,
                        (Error) open_transaction_stack);
            }
        }

    }

    /**
     * Returns the number of transactions that are open on the conglomerate.
     */
    synchronized int count() {
        return open_transactions.size();
    }

    /**
     * Returns the minimum commit id not including the given transaction object.
     * Returns Long.MAX_VALUE if there are no open transactions in the list
     * (not including the given transaction).
     */
    synchronized long minimumCommitID(Transaction transaction) {

        long minimum_commit_id = Long.MAX_VALUE;
        if (open_transactions.size() > 0) {
            // If the bottom transaction is this transaction, then go to the
            // next up from the bottom (we don't count this transaction as the
            // minimum commit_id).
            Transaction test_transaction = (Transaction) open_transactions.get(0);
            if (test_transaction != transaction) {
                minimum_commit_id = test_transaction.getCommitID();
            } else if (open_transactions.size() > 1) {
                minimum_commit_id =
                        ((Transaction) open_transactions.get(1)).getCommitID();
            }
        }

        return minimum_commit_id;

    }

    public synchronized String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("[ OpenTransactionList: ");
        for (Object open_transaction : open_transactions) {
            Transaction t = (Transaction) open_transaction;
            buf.append(t.getCommitID());
            buf.append(", ");
        }
        buf.append(" ]");
        return new String(buf);
    }

}
