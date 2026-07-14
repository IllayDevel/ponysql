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

import java.io.*;

import com.pony.database.global.BlobAccessor;
import com.pony.database.global.ByteLongObject;

/**
 * A ViewDef object is a definition of a view stored in the database.  It is
 * an object that can be easily serialized and deserialized to/from the system
 * view table.  It contains the DataTableDef that describes the characteristics
 * of the view result, and a QueryPlanNode that describes how the view can be
 * constructed.
 *
 * @author Tobias Downer
 */

public class ViewDef {

    /**
     * The DataTableDef object that describes the view column def.
     */
    private final DataTableDef view_def;

    /**
     * The QueryPlanNode that is used to evaluate the view.
     */
    private final QueryPlanNode view_query_node;

    /**
     * Constructs the ViewDef object.
     */
    public ViewDef(DataTableDef view_def, QueryPlanNode query_node) {
        this.view_def = view_def;
        this.view_query_node = query_node;
    }

    /**
     * Returns the DataTableDef for this view.
     */
    public DataTableDef getDataTableDef() {
        return view_def;
    }

    /**
     * Returns the QueryPlanNode for this view.
     */
    public QueryPlanNode getQueryPlanNode() {
        try {
            return (QueryPlanNode) view_query_node.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Clone error: " + e.getMessage(), e);
        }
    }


    /**
     * Forms this ViewDef object into a serialized ByteLongObject object that can
     * be stored in a table.
     */
    ByteLongObject serializeToBlob() {
        try {
            ByteArrayOutputStream byte_out = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(byte_out);
            // Write the version number
            out.writeInt(1);
            // Write the DataTableDef
            getDataTableDef().write(out);
            // Serialize the QueryPlanNode
            out.writeObject(getQueryPlanNode());

            out.flush();

            return new ByteLongObject(byte_out.toByteArray());

        } catch (IOException e) {
            throw new RuntimeException("IO Error: " + e.getMessage(), e);
        }

    }

    /**
     * Creates an instance of ViewDef from the serialized information stored in
     * the blob.
     */
    static ViewDef deserializeFromBlob(BlobAccessor blob) {
        InputStream blob_in = blob.getInputStream();
        try {
            ObjectInputStream in = new ObjectInputStream(blob_in);
            // Read the version
            int version = in.readInt();
            if (version == 1) {
                DataTableDef view_def = DataTableDef.read(in);
                view_def.setImmutable();
                QueryPlanNode view_plan = (QueryPlanNode) in.readObject();
                return new ViewDef(view_def, view_plan);
            } else {
                throw new IOException(
                        "Newer ViewDef version serialization: " + version);
            }

        } catch (IOException e) {
            throw new RuntimeException("IO Error: " + e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found: " + e.getMessage(), e);
        }
    }

}
