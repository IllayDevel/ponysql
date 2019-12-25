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
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import java.util.zip.DataFormatException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import com.pony.util.PagedInputStream;
import com.pony.store.Store;
import com.pony.store.Area;
import com.pony.store.MutableArea;
import com.pony.store.AreaWriter;
import com.pony.database.jdbc.AsciiReader;
import com.pony.database.jdbc.BinaryToUnicodeReader;
import com.pony.database.global.Ref;
import com.pony.database.global.BlobRef;
import com.pony.database.global.ClobRef;
import com.pony.database.global.ByteLongObject;

/**
 * A structure inside an Area that maintains the storage of any number of large
 * binary objects.  A blob store allows for the easy allocation of areas for
 * storing blob data and for reading and writing blob information via BlobRef
 * objects.
 * <p>
 * A BlobStore can be broken down to the following simplistic functions;
 * <p>
 * 1) Allocation of an area to store a new blob.<br>
 * 2) Reading the information in a Blob given a Blob reference identifier.<br>
 * 3) Reference counting to a particular Blob.<br>
 * 4) Cleaning up a Blob when no static references are left.<br>
 *
 * @author Tobias Downer
 */

final class BlobStore implements BlobStoreInterface {

    /**
     * The magic value for fixed record list structures.
     */
    private final static int MAGIC = 0x012BC53A9;

    /**
     * The outer Store object that is to contain the blob store.
     */
    private final Store store;

    /**
     * The FixedRecordList structure that maintains a list of fixed size records
     * for blob reference counting.
     */
    private final FixedRecordList fixed_list;

    /**
     * The first delete chain element.
     */
    private long first_delete_chain_record;


    /**
     * Constructs the BlobStore on the given Area object.
     */
    BlobStore(Store store) {
        this.store = store;
        fixed_list = new FixedRecordList(store, 24);
    }


    /**
     * Creates the blob store and returns a pointer in the store to the header
     * information.  This value is later used to initialize the store.
     */
    long create() throws IOException {
        // Init the fixed record list area.
        // The fixed list entries are formatted as follows;
        //  ( status (int), reference_count (int),
        //    blob_size (long), blob_pointer (long) )
        long fixed_list_p = fixed_list.create();

        // Delete chain is empty when we start
        first_delete_chain_record = -1;
        fixed_list.setReservedLong(-1);

        // Allocate a small header that contains the MAGIC, and the pointer to the
        // fixed list structure.
        AreaWriter blob_store_header = store.createArea(32);
        long blob_store_p = blob_store_header.getID();
        // Write the blob store header information
        // The magic
        blob_store_header.putInt(MAGIC);
        // The version
        blob_store_header.putInt(1);
        // The pointer to the fixed list area
        blob_store_header.putLong(fixed_list_p);
        // And finish
        blob_store_header.finish();

        // Return the pointer to the blob store header
        return blob_store_p;
    }

    /**
     * Initializes the blob store given a pointer to the blob store pointer
     * header (the value previously returned by the 'create' method).
     */
    void init(long blob_store_p) throws IOException {
        // Get the header area
        Area blob_store_header = store.getArea(blob_store_p);
        blob_store_header.position(0);
        // Read the magic
        int magic = blob_store_header.getInt();
        int version = blob_store_header.getInt();
        if (magic != MAGIC) {
            throw new IOException("MAGIC value for BlobStore is not correct.");
        }
        if (version != 1) {
            throw new IOException("version number for BlobStore is not correct.");
        }

        // Read the pointer to the fixed area
        long fixed_list_p = blob_store_header.getLong();
        // Init the FixedRecordList area
        fixed_list.init(fixed_list_p);

        // Set the delete chain
        first_delete_chain_record = fixed_list.getReservedLong();
    }


    /**
     * Simple structure used when copying blob information.
     */
    private static class CopyBlobInfo {
        int ref_count;
        long size;
        long ob_p;
    }

    /**
     * Copies all the blob data from the given BlobStore into this blob store.
     * Any blob information that already exists within this BlobStore is deleted.
     * We assume this method is called after the blob store is created or
     * initialized.
     */
    void copyFrom(StoreSystem store_system,
                  BlobStore src_blob_store) throws IOException {
        FixedRecordList src_fixed_list = src_blob_store.fixed_list;
        long node_count;
        synchronized (src_fixed_list) {
            node_count = src_fixed_list.addressableNodeCount();
        }

        synchronized (fixed_list) {

            // Make sure our fixed_list is big enough to accomodate the copied list,
            while (fixed_list.addressableNodeCount() < node_count) {
                fixed_list.increaseSize();
            }

            // We rearrange the delete chain
            long last_deleted = -1;

            // We copy blobs in groups no larger than 1024 Blobs
            final int BLOCK_WRITE_COUNT = 1024;

            int max_to_read = (int) Math.min(BLOCK_WRITE_COUNT, node_count);
            long p = 0;

            while (max_to_read > 0) {
                // (CopyBlboInfo)
                ArrayList<Object> src_copy_list = new ArrayList<>();

                synchronized (src_fixed_list) {
                    for (int i = 0; i < max_to_read; ++i) {
                        Area a = src_fixed_list.positionOnNode(p + i);
                        int status = a.getInt();
                        // If record is not deleted
                        if (status != 0x020000) {
                            CopyBlobInfo info = new CopyBlobInfo();
                            info.ref_count = a.getInt();
                            info.size = a.getLong();
                            info.ob_p = a.getLong();
                            src_copy_list.add(info);
                        } else {
                            src_copy_list.add(null);
                        }
                    }
                }

                try {
                    store.lockForWrite();

                    // We now should have a list of all records from the src to copy,
                    int sz = src_copy_list.size();
                    for (int i = 0; i < sz; ++i) {
                        CopyBlobInfo info = (CopyBlobInfo) src_copy_list.get(i);
                        MutableArea a = fixed_list.positionOnNode(p + i);
                        // Either set a deleted entry or set the entry with a copied blob.
                        if (info == null) {
                            a.putInt(0x020000);
                            a.putInt(0);
                            a.putLong(-1);
                            a.putLong(last_deleted);
                            a.checkOut();
                            last_deleted = p + i;
                        } else {
                            // Get the Area containing the blob header data in the source
                            // store
                            Area src_blob_header = src_blob_store.store.getArea(info.ob_p);
                            // Read the information from the header,
                            int res = src_blob_header.getInt();
                            int type = src_blob_header.getInt();
                            long total_block_size = src_blob_header.getLong();
                            long total_block_pages = src_blob_header.getLong();

                            // Allocate a new header
                            AreaWriter dst_blob_header = store.createArea(
                                    4 + 4 + 8 + 8 + (total_block_pages * 8));
                            long new_ob_header_p = dst_blob_header.getID();
                            // Copy information into the header
                            dst_blob_header.putInt(res);
                            dst_blob_header.putInt(type);
                            dst_blob_header.putLong(total_block_size);
                            dst_blob_header.putLong(total_block_pages);

                            // Allocate and copy each page,
                            for (int n = 0; n < total_block_pages; ++n) {
                                // Get the block information
                                long block_p = src_blob_header.getLong();
                                long new_block_p;
                                // Copy the area if the block id is not -1
                                if (block_p != -1) {
                                    Area src_block = src_blob_store.store.getArea(block_p);
                                    int block_type = src_block.getInt();
                                    int block_size = src_block.getInt();
                                    // Copy a new block,
                                    int new_block_size = block_size + 4 + 4;
                                    AreaWriter dst_block_p = store.createArea(new_block_size);
                                    new_block_p = dst_block_p.getID();
                                    src_block.position(0);
                                    src_block.copyTo(dst_block_p, new_block_size);
                                    // And finish
                                    dst_block_p.finish();
                                } else {
                                    new_block_p = -1;
                                }
                                // Write the new header
                                dst_blob_header.putLong(new_block_p);
                            }

                            // And finish 'dst_blob_header'
                            dst_blob_header.finish();

                            // Set up the data in the fixed list
                            a.putInt(1);
                            // Note all the blobs are written with 0 reference count.
                            a.putInt(0);
                            a.putLong(info.size);
                            a.putLong(new_ob_header_p);
                            // Check out the changes
                            a.checkOut();
                        }
                    }

                } finally {
                    store.unlockForWrite();
                }

                node_count -= max_to_read;
                p += max_to_read;
                max_to_read = (int) Math.min(BLOCK_WRITE_COUNT, node_count);

                // Set a checkpoint in the destination store system so we write out
                // all pending changes from the log
                store_system.setCheckPoint();

            }

            // Set the delete chain
            first_delete_chain_record = last_deleted;
            fixed_list.setReservedLong(last_deleted);

        } // synchronized (fixed_list)

    }

    /**
     * Convenience method that converts the given String into a ClobRef
     * object and pushes it into the given BlobStore object.
     */
    ClobRef putStringInBlobStore(String str) throws IOException {
        final int BUF_SIZE = 64 * 1024;

        int size = str.length();

        byte type = 4;
        // Enable compression (ISSUE: Should this be enabled by default?)
        type = (byte) (type | 0x010);

        ClobRef ref = (ClobRef) allocateLargeObject(type, size * 2);
        byte[] buf = new byte[BUF_SIZE];
        long p = 0;
        int str_i = 0;
        while (size > 0) {
            int to_write = Math.min(BUF_SIZE / 2, size);
            int buf_i = 0;
            for (int i = 0; i < to_write; ++i) {
                char c = str.charAt(str_i);
                buf[buf_i] = (byte) (c >> 8);
                ++buf_i;
                buf[buf_i] = (byte) c;
                ++buf_i;
                ++str_i;
            }
            ref.write(p, buf, buf_i);
            size -= to_write;
            p += to_write * 2;
        }

        ref.complete();

        return ref;
    }

    /**
     * Convenience method that converts the given ByteLongObject into a
     * BlobRef object and pushes it into the given BlobStore object.
     */
    BlobRef putByteLongObjectInBlobStore(ByteLongObject blob) throws IOException {

        final int BUF_SIZE = 64 * 1024;

        byte[] src_buf = blob.getByteArray();
        final int size = src_buf.length;
        BlobRef ref = (BlobRef) allocateLargeObject((byte) 2, size);

        byte[] copy_buf = new byte[BUF_SIZE];
        int offset = 0;
        int to_write = Math.min(BUF_SIZE, size);

        while (to_write > 0) {
            System.arraycopy(src_buf, offset, copy_buf, 0, to_write);
            ref.write(offset, copy_buf, to_write);

            offset += to_write;
            to_write = Math.min(BUF_SIZE, (size - offset));
        }

        ref.complete();

        return ref;
    }

    /**
     * Finds a free place to add a record and returns an index to the record here.
     * This may expand the record space as necessary if there are no free record
     * slots to use.
     * <p>
     * NOTE: Unfortunately this is cut-and-paste from the way
     *   V2MasterTableDataSource manages recycled elements.
     */
    private long addToRecordList(long record_p) throws IOException {

        synchronized (fixed_list) {
            // If there is no free deleted records in the delete chain,
            if (first_delete_chain_record == -1) {

                // Increase the size of the list structure.
                fixed_list.increaseSize();
                // The start record of the new size
                int new_block_number = fixed_list.listBlockCount() - 1;
                long start_index = fixed_list.listBlockFirstPosition(new_block_number);
                long size_of_block = fixed_list.listBlockNodeCount(new_block_number);
                // The Area object for the new position
                MutableArea a = fixed_list.positionOnNode(start_index);

                a.putInt(0);
                a.putInt(0);
                a.putLong(-1);  // Initially unknown size
                a.putLong(record_p);
                // Set the rest of the block as deleted records
                for (long n = 1; n < size_of_block - 1; ++n) {
                    a.putInt(0x020000);
                    a.putInt(0);
                    a.putLong(-1);
                    a.putLong(start_index + n + 1);
                }
                // The last block is end of delete chain.
                a.putInt(0x020000);
                a.putInt(0);
                a.putLong(-1);
                a.putLong(-1);
                // Check out the changes.
                a.checkOut();
                // And set the new delete chain
                first_delete_chain_record = start_index + 1;
                // Set the reserved area
                fixed_list.setReservedLong(first_delete_chain_record);
//        // Flush the changes to the store
//        store.flush();

                // Return pointer to the record we just added.
                return start_index;

            } else {

                // Pull free block from the delete chain and recycle it.
                long recycled_record = first_delete_chain_record;
                MutableArea block = fixed_list.positionOnNode(recycled_record);
                int rec_pos = block.position();
                // Status of the recycled block
                int status = block.getInt();
                if ((status & 0x020000) == 0) {
                    throw new Error("Assertion failed: record is not deleted!");
                }
                // Reference count (currently unused in delete chains).
                block.getInt();
                // The size (should be -1);
                block.getLong();
                // The pointer to the next in the chain.
                long next_chain = block.getLong();
                first_delete_chain_record = next_chain;
                // Update the first_delete_chain_record field in the header
                fixed_list.setReservedLong(first_delete_chain_record);
                // Update the block
                block.position(rec_pos);
                block.putInt(0);
                block.putInt(0);
                block.putLong(-1);    // Initially unknown size
                block.putLong(record_p);
                // Check out the changes
                block.checkOut();

                return recycled_record;
            }
        }

    }


    /**
     * Allocates an area in the store for a large binary object to be stored.
     * After the blob area is allocated the blob may be written.  This returns
     * a BlobRef object for future access to the blob.
     * <p>
     * A newly allocated blob is read and write enabled.  A call to the
     * 'completeBlob' method must be called to finalize the blob at which point
     * the blob becomes a static read-only object.
     */
    Ref allocateLargeObject(byte type, long size) throws IOException {
        if (size < 0) {
            throw new IOException("Negative blob size not allowed.");
        }

        try {
            store.lockForWrite();

            // Allocate the area (plus header area) for storing the blob pages
            long page_count = ((size - 1) / (64 * 1024)) + 1;
            AreaWriter blob_area = store.createArea((page_count * 8) + 24);
            long blob_p = blob_area.getID();
            // Set up the area header
            blob_area.putInt(0);           // Reserved for future
            blob_area.putInt(type);
            blob_area.putLong(size);
            blob_area.putLong(page_count);
            // Initialize the empty blob area
            for (long i = 0; i < page_count; ++i) {
                blob_area.putLong(-1);
            }
            // And finish
            blob_area.finish();

            // Update the fixed_list and return the record number for this blob
            long reference_id = addToRecordList(blob_p);
            byte st_type = (byte) (type & 0x0F);
            if (st_type == 2) {
                // Create a BlobRef implementation that can access this blob
                return new BlobRefImpl(reference_id, type, size, true);
            } else if (st_type == 3) {
                return new ClobRefImpl(reference_id, type, size, true);
            } else if (st_type == 4) {
                return new ClobRefImpl(reference_id, type, size, true);
            } else {
                throw new IOException("Unknown large object type");
            }

        } finally {
            store.unlockForWrite();
        }

    }

    /**
     * Returns a Ref object that allows read-only access to a large object in this
     * blob store.
     */
    public Ref getLargeObject(long reference_id) throws IOException {

        long blob_p;
        long size;
        synchronized (fixed_list) {

            // Assert that the blob reference id given is a valid range
            if (reference_id < 0 ||
                    reference_id >= fixed_list.addressableNodeCount()) {
                throw new IOException("reference_id is out of range.");
            }

            // Position on this record
            Area block = fixed_list.positionOnNode(reference_id);
            // Read the information in the fixed record
            int status = block.getInt();
            // Assert that the status is not deleted
            if ((status & 0x020000) != 0) {
                throw new Error("Assertion failed: record is deleted!");
            }
            // Get the reference count
            int reference_count = block.getInt();
            // Get the total size of the blob
            size = block.getLong();
            // Get the blob pointer
            blob_p = block.getLong();

        }

        Area blob_area = store.getArea(blob_p);
        blob_area.position(0);
        blob_area.getInt();  // (reserved)
        // Read the type
        byte type = (byte) blob_area.getInt();
        // The size of the block
        long block_size = blob_area.getLong();
        // The number of pages in the blob
        long page_count = blob_area.getLong();

        if (type == (byte) 2) {
            // Create a new BlobRef object.
            return new BlobRefImpl(reference_id, type, size, false);
        } else {
            // Create a new ClobRef object.
            return new ClobRefImpl(reference_id, type, size, false);
        }
    }

    /**
     * Call this to complete a blob in the store after a blob has been completely
     * written.  Only BlobRef implementations returned by the 'allocateBlob'
     * method are accepted.
     */
    void completeBlob(AbstractRef ref) throws IOException {
        // Assert that the BlobRef is open and allocated
        ref.assertIsOpen();
        // Get the blob reference id (reference to the fixed record list).
        long blob_reference_id = ref.getID();

        synchronized (fixed_list) {

            // Update the record in the fixed list.
            MutableArea block = fixed_list.positionOnNode(blob_reference_id);
            // Record the position
            int rec_pos = block.position();
            // Read the information in the fixed record
            int status = block.getInt();
            // Assert that the status is open
            if (status != 0) {
                throw new IOException("Assertion failed: record is not open.");
            }
            int reference_count = block.getInt();
            long size = block.getLong();
            long page_count = block.getLong();

            try {
                store.lockForWrite();

                // Set the fixed blob record as complete.
                block.position(rec_pos);
                // Write the new status
                block.putInt(1);
                // Write the reference count
                block.putInt(0);
                // Write the completed size
                block.putLong(ref.getRawSize());
                // Write the pointer
                block.putLong(page_count);
                // Check out the change
                block.checkOut();

            } finally {
                store.unlockForWrite();
            }

        }
        // Now the blob has been finalized so change the state of the BlobRef
        // object.
        ref.close();

    }

    /**
     * Tells the BlobStore that a static reference has been established in a
     * table to the blob referenced by the given id.  This is used to count
     * references to a blob, and possibly clean up a blob if there are no
     * references remaining to it.
     * <p>
     * NOTE: It is the responsibility of the callee to establish a 'lockForWrite'
     *   lock on the store before this is used.
     */
    public void establishReference(long blob_reference_id) {
        try {
            synchronized (fixed_list) {
                // Update the record in the fixed list.
                MutableArea block = fixed_list.positionOnNode(blob_reference_id);
                // Record the position
                int rec_pos = block.position();
                // Read the information in the fixed record
                int status = block.getInt();
                // Assert that the status is static
                if (status != 1) {
                    throw new RuntimeException("Assertion failed: record is not static.");
                }
                int reference_count = block.getInt();

                // Set the fixed blob record as complete.
                block.position(rec_pos + 4);
                // Write the reference count + 1
                block.putInt(reference_count + 1);
                // Check out the change
                block.checkOut();
            }
//      // Flush all changes to the store.
//      store.flush();
        } catch (IOException e) {
            throw new RuntimeException("IO Error: " + e.getMessage());
        }
    }

    /**
     * Tells the BlobStore that a static reference has been released to the
     * given blob.  This would typically be called when the row in the database
     * is removed.
     * <p>
     * NOTE: It is the responsibility of the callee to establish a 'lockForWrite'
     *   lock on the store before this is used.
     */
    public void releaseReference(long blob_reference_id) {
        try {
            synchronized (fixed_list) {
                // Update the record in the fixed list.
                MutableArea block = fixed_list.positionOnNode(blob_reference_id);
                // Record the position
                int rec_pos = block.position();
                // Read the information in the fixed record
                int status = block.getInt();
                // Assert that the status is static
                if (status != 1) {
                    throw new RuntimeException("Assertion failed: " +
                            "Record is not static (status = " + status + ")");
                }
                int reference_count = block.getInt();
                if (reference_count == 0) {
                    throw new RuntimeException(
                            "Releasing when Blob reference counter is at 0.");
                }

                long object_size = block.getLong();
                long object_p = block.getLong();

                // If reference count == 0 then we need to free all the resources
                // associated with this Blob in the blob store.
                if ((reference_count - 1) == 0) {
                    // Free the resources associated with this object.
                    Area blob_area = store.getArea(object_p);
                    blob_area.getInt();
                    byte type = (byte) blob_area.getInt();
                    long total_size = blob_area.getLong();
                    long page_count = blob_area.getLong();
                    // Free all of the pages in this blob.
                    for (long i = 0; i < page_count; ++i) {
                        long page_p = blob_area.getLong();
                        if (page_p > 0) {
                            store.deleteArea(page_p);
                        }
                    }
                    // Free the blob area object itself.
                    store.deleteArea(object_p);
                    // Write out the blank record.
                    block.position(rec_pos);
                    block.putInt(0x020000);
                    block.putInt(0);
                    block.putLong(-1);
                    block.putLong(first_delete_chain_record);
                    // CHeck out these changes
                    block.checkOut();
                    first_delete_chain_record = blob_reference_id;
                    // Update the first_delete_chain_record field in the header
                    fixed_list.setReservedLong(first_delete_chain_record);
                } else {
                    // Simply decrement the reference counter for this record.
                    block.position(rec_pos + 4);
                    // Write the reference count - 1
                    block.putInt(reference_count - 1);
                    // Check out this change
                    block.checkOut();
                }

            }
//      // Flush all changes to the store.
//      store.flush();
        } catch (IOException e) {
            throw new RuntimeException("IO Error: " + e.getMessage());
        }
    }


    /**
     * Reads a section of the blob referenced by the given id, offset and length
     * into the byte array.
     */
    private void readBlobByteArray(long reference_id, long offset,
                                   byte[] buf, int off, int length) throws IOException {

        // ASSERT: Read and write position must be 64K aligned.
        if (offset % (64 * 1024) != 0) {
            throw new RuntimeException("Assert failed: offset is not 64k aligned.");
        }
        // ASSERT: Length is less than or equal to 64K
        if (length > (64 * 1024)) {
            throw new RuntimeException("Assert failed: length is greater than 64K.");
        }

        int status;
        int reference_count;
        long size;
        long blob_p;

        synchronized (fixed_list) {

            // Assert that the blob reference id given is a valid range
            if (reference_id < 0 ||
                    reference_id >= fixed_list.addressableNodeCount()) {
                throw new IOException("blob_reference_id is out of range.");
            }

            // Position on this record
            Area block = fixed_list.positionOnNode(reference_id);
            // Read the information in the fixed record
            status = block.getInt();
            // Assert that the status is not deleted
            if ((status & 0x020000) != 0) {
                throw new Error("Assertion failed: record is deleted!");
            }
            // Get the reference count
            reference_count = block.getInt();
            // Get the total size of the blob
            size = block.getLong();
            // Get the blob pointer
            blob_p = block.getLong();

        }

        // Assert that the area being read is within the bounds of the blob
        if (offset < 0 || offset + length > size) {
            throw new IOException("Blob invalid read.  offset = " + offset +
                    ", length = " + length);
        }

        // Open an Area into the blob
        Area blob_area = store.getArea(blob_p);
        blob_area.getInt();
        byte type = (byte) blob_area.getInt();

        // Convert to the page number
        long page_number = (offset / (64 * 1024));
        blob_area.position((int) ((page_number * 8) + 24));
        long page_p = blob_area.getLong();

        // Read the page
        Area page_area = store.getArea(page_p);
        page_area.position(0);
        int page_type = page_area.getInt();
        int page_size = page_area.getInt();
        if ((type & 0x010) != 0) {
            // The page is compressed
            byte[] page_buf = new byte[page_size];
            page_area.get(page_buf, 0, page_size);
            Inflater inflater = new Inflater();
            inflater.setInput(page_buf, 0, page_size);
            try {
                int result_length = inflater.inflate(buf, off, length);
                if (result_length != length) {
                    throw new RuntimeException(
                            "Assert failed: decompressed length is incorrect.");
                }
            } catch (DataFormatException e) {
                throw new IOException("ZIP Data Format Error: " + e.getMessage());
            }
            inflater.end();
        } else {
            // The page is not compressed
            page_area.get(buf, off, length);
        }

    }

    /**
     * Writes a section of the blob referenced by the given id, offset and
     * length to the byte array.  Note that this does not perform any checks on
     * whether we are allowed to write to this blob.
     */
    private void writeBlobByteArray(long reference_id, long offset,
                                    byte[] buf, int length) throws IOException {

        // ASSERT: Read and write position must be 64K aligned.
        if (offset % (64 * 1024) != 0) {
            throw new RuntimeException("Assert failed: offset is not 64k aligned.");
        }
        // ASSERT: Length is less than or equal to 64K
        if (length > (64 * 1024)) {
            throw new RuntimeException("Assert failed: length is greater than 64K.");
        }

        int status;
        int reference_count;
        long size;
        long blob_p;

        synchronized (fixed_list) {

            // Assert that the blob reference id given is a valid range
            if (reference_id < 0 ||
                    reference_id >= fixed_list.addressableNodeCount()) {
                throw new IOException("blob_reference_id is out of range.");
            }

            // Position on this record
            Area block = fixed_list.positionOnNode(reference_id);
            // Read the information in the fixed record
            status = block.getInt();
            // Assert that the status is not deleted
            if ((status & 0x020000) != 0) {
                throw new Error("Assertion failed: record is deleted!");
            }
            // Get the reference count
            reference_count = block.getInt();
            // Get the total size of the blob
            size = block.getLong();
            // Get the blob pointer
            blob_p = block.getLong();

        }

        // Open an Area into the blob
        MutableArea blob_area = store.getMutableArea(blob_p);
        blob_area.getInt();
        byte type = (byte) blob_area.getInt();
        size = blob_area.getLong();

        // Assert that the area being read is within the bounds of the blob
        if (offset < 0 || offset + length > size) {
            throw new IOException("Blob invalid write.  offset = " + offset +
                    ", length = " + length + ", size = " + size);
        }

        // Convert to the page number
        long page_number = (offset / (64 * 1024));
        blob_area.position((int) ((page_number * 8) + 24));
        long page_p = blob_area.getLong();

        // Assert that 'page_p' is -1
        if (page_p != -1) {
            // This means we are trying to rewrite a page we've already written
            // before.
            throw new RuntimeException("Assert failed: page_p is not -1");
        }

        // Is the compression bit set?
        byte[] to_write;
        int write_length;
        if ((type & 0x010) != 0) {
            // Yes, compression
            Deflater deflater = new Deflater();
            deflater.setInput(buf, 0, length);
            deflater.finish();
            to_write = new byte[65 * 1024];
            write_length = deflater.deflate(to_write);
        } else {
            // No compression
            to_write = buf;
            write_length = length;
        }

        try {
            store.lockForWrite();

            // Allocate and write the page.
            AreaWriter page_area = store.createArea(write_length + 8);
            page_p = page_area.getID();
            page_area.putInt(1);
            page_area.putInt(write_length);
            page_area.put(to_write, 0, write_length);
            // Finish this page
            page_area.finish();

            // Update the page in the header.
            blob_area.position((int) ((page_number * 8) + 24));
            blob_area.putLong(page_p);
            // Check out this change.
            blob_area.checkOut();

        } finally {
            store.unlockForWrite();
        }

    }

    /**
     * An InputStream implementation that reads from the underlying blob data as
     * fixed size pages.
     */
    private class BLOBInputStream extends PagedInputStream {

        final static int B_SIZE = 64 * 1024;

        private final long reference_id;

        public BLOBInputStream(final long reference_id, final long size) {
            super(B_SIZE, size);
            this.reference_id = reference_id;
        }

        public void readPageContent(byte[] buf, long pos, int length)
                throws IOException {
            readBlobByteArray(reference_id, pos, buf, 0, length);
        }

    }

    /**
     * An abstract implementation of a Ref object for referencing large objects
     * in this blob store.
     */
    private class AbstractRef {

        /**
         * The reference identifier.  This is a pointer into the fixed list
         * structure.
         */
        protected final long reference_id;

        /**
         * The total size of the large object in bytes.
         */
        protected final long size;

        /**
         * The type of large object.
         */
        protected final byte type;

        /**
         * Set to true if this large object is open for writing, otherwise the
         * object is an immutable static object.
         */
        private boolean open_for_write;

        /**
         * Constructs the Ref implementation.
         */
        AbstractRef(long reference_id, byte type, long size,
                    boolean open_for_write) {
            this.reference_id = reference_id;
            this.size = size;
            this.type = type;
            this.open_for_write = open_for_write;
        }

        /**
         * Asserts that this blob is open for writing.
         */
        void assertIsOpen() {
            if (!open_for_write) {
                throw new Error("Large object ref is newly allocated.");
            }
        }

        public long getRawSize() {
            return size;
        }

        /**
         * Marks this large object as closed to write operations.
         */
        void close() {
            open_for_write = false;
        }

        public int length() {
            return (int) size;
        }

        public long getID() {
            return reference_id;
        }

        public byte getType() {
            return type;
        }

        public void read(long offset, byte[] buf, int length) throws IOException {
            // Reads the section of the blob into the given buffer byte array at the
            // given offset of the blob.
            readBlobByteArray(reference_id, offset, buf, 0, length);
        }

        public void write(long offset, byte[] buf, int length) throws IOException {
            if (open_for_write) {
                writeBlobByteArray(reference_id, offset, buf, length);
            } else {
                throw new IOException("Blob is read-only.");
            }
        }

        public void complete() throws IOException {
            completeBlob(this);
        }

    }

    /**
     * An implementation of ClobRef used to represent a reference to a large
     * character object inside this blob store.
     */
    private class ClobRefImpl extends AbstractRef implements ClobRef {

        /**
         * Constructs the ClobRef implementation.
         */
        ClobRefImpl(long reference_id, byte type, long size,
                    boolean open_for_write) {
            super(reference_id, type, size, open_for_write);
        }

        // ---------- Implemented from ClobRef ----------

        public int length() {
            byte st_type = (byte) (type & 0x0F);
            if (st_type == 3) {
                return (int) size;
            } else if (st_type == 4) {
                return (int) (size / 2);
            } else {
                throw new RuntimeException("Unknown type.");
            }
        }

        public Reader getReader() {
            byte st_type = (byte) (type & 0x0F);
            if (st_type == 3) {
                return new AsciiReader(new BLOBInputStream(reference_id, size));
            } else if (st_type == 4) {
                return new BinaryToUnicodeReader(
                        new BLOBInputStream(reference_id, size));
            } else {
                throw new RuntimeException("Unknown type.");
            }
        }

        public String toString() {
            final int BUF_SIZE = 8192;
            Reader r = getReader();
            StringBuffer buf = new StringBuffer(length());
            char[] c = new char[BUF_SIZE];
            try {
                while (true) {
                    int has_read = r.read(c, 0, BUF_SIZE);
                    if (has_read == 0 || has_read == -1) {
                        return new String(buf);
                    }
                    buf.append(c);
                }
            } catch (IOException e) {
                throw new RuntimeException("IO Error: " + e.getMessage());
            }
        }

    }

    /**
     * An implementation of BlobRef used to represent a blob reference inside this
     * blob store.
     */
    private class BlobRefImpl extends AbstractRef implements BlobRef {

        /**
         * Constructs the BlobRef implementation.
         */
        BlobRefImpl(long reference_id, byte type, long size,
                    boolean open_for_write) {
            super(reference_id, type, size, open_for_write);
        }

        // ---------- Implemented from BlobRef ----------

        public InputStream getInputStream() {
            return new BLOBInputStream(reference_id, size);
        }

    }

}

