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
            throw new Error("Clone error: " + e.getMessage());
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
            throw new Error("IO Error: " + e.getMessage());
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
            throw new Error("IO Error: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new Error("Class not found: " + e.getMessage());
        }
    }

}

