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

package com.pony.database;

/**
 * A listener that is notified of table modification events made by a
 * transaction, both immediately inside a transaction and when a transaction
 * commits.  These events can occur either immediately before or immediately
 * after the data is modified or during a commit.
 *
 * @author Tobias Downer
 */

public interface TransactionModificationListener {

    /**
     * An action for when changes to a table are committed.  This event occurs
     * after constraint checks, and before the change is actually committed to
     * the database.  If this method generates an exception then the change
     * is rolled back and any changes made by the transaction are lost.  This
     * action is generated inside a 'commit lock' of the conglomerate, and
     * therefore care should be taken with the performance of this method.
     * <p>
     * The event object provides access to a SimpleTransaction object that is a
     * read-only view of the database in its committed state (if this operation
     * is successful).  The transaction can be used to perform any last minute
     * deferred constraint checks.
     * <p>
     * This action is useful for last minute abortion of a transaction, or for
     * updating cache information.  It can not be used as a triggering mechanism
     * and should never call back to user code.
     */
    void tableCommitChange(TableCommitModificationEvent event);

}

