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

package com.pony.database.global;

/**
 * An interface that provides access to basic information about a BLOB so that
 * we may compare BLOBs implemented in different ways.
 *
 * @author Tobias Downer
 */

public interface BlobAccessor {

    /**
     * Returns the size of the BLOB.
     */
    int length();

    /**
     * Returns an InputStream that allows us to read the contents of the blob
     * from start to finish.  This object should be wrapped in a
     * BufferedInputStream if 'read()' type efficiency is required.
     */
    java.io.InputStream getInputStream();

}

