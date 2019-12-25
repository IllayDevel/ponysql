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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;

import com.pony.util.TimeFrame;

/**
 * Encapsulates the information in a query to the database.  This object is
 * used in QueryAgent.
 *
 * @author Tobias Downer
 */

public class Query implements Cloneable {

    /**
     * The string to query.
     */
    private String query_string;

    /**
     * The parameters of the query (if any).
     */
    private ArrayList parameters;

    /**
     * Constructs the query.
     */
    public Query(String query) {
        this.query_string = query;
    }

    /**
     * Sets a parameter.
     */
    private void internalSet(int index, Object ob) {
        if (parameters == null) {
            parameters = new ArrayList();
        }
        for (int i = parameters.size(); i < index; ++i) {
            parameters.add(null);
        }
        Object old = parameters.set(index - 1, ob);
        if (old != null) {
//      Debug.write(Debug.WARNING, this,
//                  "Setting over a previously set parameter.");
        }
    }

    /**
     * Returns the query string.
     */
    public String getString() {
        return query_string;
    }

    /**
     * Returns the number of parameters.
     */
    public int parameterCount() {
        if (parameters == null) {
            return 0;
        }
        return parameters.size();
    }

    /**
     * Returns parameters number 'n' where 0 is the first parameters, etc.
     */
    public Object getParameter(int index) {
        return parameters.get(index);
    }

    /**
     * Returns a copy of this Query object but with a different query string.
     */
    public Query changeSQL(String sql) {
        try {
            Query query = (Query) clone();
            query.query_string = sql;
            return query;
        } catch (CloneNotSupportedException e) {
            throw new Error(e.getMessage());
        }
    }

    /**
     * For debugging.
     */
    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append("Query: " + query_string + "\n");
        str.append("Parameters: ");
        for (int i = 0; i < parameterCount(); ++i) {
            str.append(getParameter(i));
            str.append(", ");
        }
        return new String(str);
    }


    // ---------- Methods for adding different types of parameters ----------
    // NOTE: For all these methods, para_index = 1 is the first parameters,
    //   2 is the second, etc.

    public void setString(int para_index, String str) {
        internalSet(para_index, str);
    }

    public void setBoolean(int para_index, boolean val) {
        internalSet(para_index, val);
    }

    public void setBigDecimal(int para_index, BigDecimal val) {
        internalSet(para_index, val);
    }

    public void setInt(int para_index, int val) {
        internalSet(para_index, new BigDecimal(val));
    }

    public void setLong(int para_index, long val) {
        internalSet(para_index, new BigDecimal(val));
    }

    public void setDouble(int para_index, double val) {
        internalSet(para_index, new BigDecimal(val));
    }

    public void setDate(int para_index, Date val) {
        internalSet(para_index, val);
    }

    public void setTimeFrame(int para_index, TimeFrame val) {
        internalSet(para_index, val.getPeriod());
    }

    public void setObject(int para_index, Object val) {
        if (val == null ||
                val instanceof BigDecimal ||
                val instanceof String ||
                val instanceof Date ||
                val instanceof Boolean) {
            internalSet(para_index, val);
        } else if (val instanceof TimeFrame) {
            setTimeFrame(para_index, (TimeFrame) val);
        } else if (val instanceof Integer) {
            internalSet(para_index, new BigDecimal((Integer) val));
        } else if (val instanceof Long) {
            internalSet(para_index, new BigDecimal((Long) val));
        } else if (val instanceof Double) {
            internalSet(para_index, new BigDecimal((Double) val));
        }
        // Default behaviour for unknown objects is to cast as a String
        // parameter.
        else {
            setString(para_index, val.toString());
        }
    }

}
