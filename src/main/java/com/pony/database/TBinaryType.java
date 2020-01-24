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

package com.pony.database;

import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.IOException;

import com.pony.database.global.ByteLongObject;
import com.pony.database.global.BlobRef;
import com.pony.database.global.BlobAccessor;

/**
 * An implementation of TType for a binary block of data.
 *
 * @author Tobias Downer
 */

public class TBinaryType extends TType {

    static final long serialVersionUID = 5141996433600529406L;

    /**
     * This constrained size of the binary block of data or -1 if there is no
     * size limit.
     */
    private final int max_size;

    /**
     * Constructs the type.
     */
    public TBinaryType(int sql_type, int max_size) {
        super(sql_type);
        this.max_size = max_size;
    }

    /**
     * Returns the maximum size of this binary type.
     */
    public int getMaximumSize() {
        return max_size;
    }

    // ---------- Static utility method for comparing blobs ----------

    /**
     * Utility method for comparing one blob with another.  Uses the
     * BlobAccessor interface to compare the blobs.  This will collate larger
     * blobs higher than smaller blobs.
     */
    static int compareBlobs(BlobAccessor blob1, BlobAccessor blob2) {
        // We compare smaller sized blobs before larger sized blobs
        int c = blob1.length() - blob2.length();
        if (c != 0) {
            return c;
        } else {
            // Size of the blobs are the same, so find the first non equal byte in
            // the byte array and return the difference between the two.  eg.
            // compareTo({ 0, 0, 0, 1 }, { 0, 0, 0, 3 }) == -3

            int len = blob1.length();

            InputStream b1 = blob1.getInputStream();
            InputStream b2 = blob2.getInputStream();
            try {
                BufferedInputStream bin1 = new BufferedInputStream(b1);
                BufferedInputStream bin2 = new BufferedInputStream(b2);
                while (len > 0) {
                    c = bin1.read() - bin2.read();
                    if (c != 0) {
                        return c;
                    }
                    --len;
                }

                return 0;
            } catch (IOException e) {
                throw new RuntimeException("IO Error when comparing blobs: " +
                        e.getMessage());
            }
        }
    }

    // ---------- Implemented from TType ----------

    public boolean comparableTypes(TType type) {
        return (type instanceof BlobAccessor);
    }

    public int compareObs(Object ob1, Object ob2) {
        if (ob1 == ob2) {
            return 0;
        }

        BlobAccessor blob1 = (BlobAccessor) ob1;
        BlobAccessor blob2 = (BlobAccessor) ob2;

        return compareBlobs(blob1, blob2);
    }

    public int calculateApproximateMemoryUse(Object ob) {
        if (ob != null) {
            if (ob instanceof BlobRef) {
                return 256;
            } else {
                return ((ByteLongObject) ob).length() + 24;
            }
        } else {
            return 32;
        }
    }

    public Class javaClass() {
        return BlobAccessor.class;
    }

}
