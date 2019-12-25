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

/**
 * A set of privileges to grant a user for an object.
 *
 * @author Tobias Downer
 */

public class Privileges {

    /**
     * The number of bits available to set.
     */
    final static int BIT_COUNT = 11;

    /**
     * The bit mask.  There are currently 11 used bits, so this has all 11 bits
     * set.
     */
    final static int BIT_MASK = (1 << BIT_COUNT) - 1;

    // ---------- Statics ----------

    /**
     * The priv to allow full access to the database object.  If this is used,
     * it should be the only privilege added.
     */
    public final static int ALL = BIT_MASK;

    /**
     * The priv to SELECT from a database object.
     */
    public final static int SELECT = 0x01;

    /**
     * The priv to DELETE from a database object.
     */
    public final static int DELETE = 0x02;

    /**
     * The priv to UPDATE a database object.
     */
    public final static int UPDATE = 0x04;

    /**
     * The priv to INSERT to a database object.
     */
    public final static int INSERT = 0x08;

    /**
     * The priv to REFERENCE a database object.
     */
    public final static int REFERENCES = 0x010;

    /**
     * The priv to see statistics on a database object.
     */
    public final static int USAGE = 0x020;

    /**
     * The priv to compact a database object.
     */
    public final static int COMPACT = 0x040;

    /**
     * The priv to create objects (only applicable for SCHEMA grant objects).
     */
    public final static int CREATE = 0x080;

    /**
     * The priv to alter objects (only applicable for SCHEMA grant objects).
     */
    public final static int ALTER = 0x0100;

    /**
     * The priv to drop objects (only applicable for SCHEMA grant objects).
     */
    public final static int DROP = 0x0200;

    /**
     * The priv to view objects in a schema (only applicable for SCHEMA grant
     * objects).
     */
    public final static int LIST = 0x0400;


    // ---------- Members ----------

    /**
     * The priv bit map.
     */
    private int privs;

    /**
     * Constructor.
     */
    private Privileges(int privs) {
        this.privs = privs & BIT_MASK;
    }

    public Privileges() {
        this(0);
    }

    /**
     * Adds a privilege and returns a new Privileges object with the new priv
     * set.
     */
    public Privileges add(int priv) {
        return new Privileges(privs | priv);
    }

    /**
     * Removes a privilege with a column list parameter.
     */
    public Privileges remove(int priv) {
        int and_priv = (privs & priv);
        return new Privileges(privs ^ and_priv);
    }

    /**
     * Removes the given privileges from this privileges object and returns the
     * new privileges object.
     */
    public Privileges remove(Privileges privs) {
        return remove(privs.privs);
    }

    /**
     * Returns true if this privileges permits the given priv.
     */
    public boolean permits(int priv) {
        return (privs & priv) != 0;
    }

    /**
     * Merges privs from the given privilege object with this set of privs.
     * This performs an OR on all the attributes in the set.  If the entry
     * does not exist in this set then it is added.
     */
    public Privileges merge(Privileges in_privs) {
        return add(in_privs.privs);
    }

    /**
     * Returns true if this Privileges object contains no priv entries.
     */
    public boolean isEmpty() {
        return privs == 0;
    }

    /**
     * Returns a String that represents the given priv bit.
     */
    static String formatPriv(int priv) {
        if ((priv & SELECT) != 0) {
            return "SELECT";
        } else if ((priv & DELETE) != 0) {
            return "DELETE";
        } else if ((priv & UPDATE) != 0) {
            return "UPDATE";
        } else if ((priv & INSERT) != 0) {
            return "INSERT";
        } else if ((priv & REFERENCES) != 0) {
            return "REFERENCES";
        } else if ((priv & USAGE) != 0) {
            return "USAGE";
        } else if ((priv & COMPACT) != 0) {
            return "COMPACT";
        } else if ((priv & CREATE) != 0) {
            return "CREATE";
        } else if ((priv & ALTER) != 0) {
            return "ALTER";
        } else if ((priv & DROP) != 0) {
            return "DROP";
        } else if ((priv & LIST) != 0) {
            return "LIST";
        } else {
            throw new Error("Not priv bit set.");
        }
    }

    /**
     * Given a string, returns the priv bit for it.
     */
    public static int parseString(String priv) {
        if (priv.equals("SELECT")) {
            return SELECT;
        } else if (priv.equals("DELETE")) {
            return DELETE;
        } else if (priv.equals("UPDATE")) {
            return UPDATE;
        } else if (priv.equals("INSERT")) {
            return INSERT;
        } else if (priv.equals("REFERENCES")) {
            return REFERENCES;
        } else if (priv.equals("USAGE")) {
            return USAGE;
        } else if (priv.equals("COMPACT")) {
            return COMPACT;
        } else if (priv.equals("CREATE")) {
            return CREATE;
        } else if (priv.equals("ALTER")) {
            return ALTER;
        } else if (priv.equals("DROP")) {
            return DROP;
        } else if (priv.equals("LIST")) {
            return LIST;
        } else {
            throw new Error("Priv not recognised.");
        }
    }

    /**
     * Returns this Privileges object as an encoded int bit array.
     */
    public int toInt() {
        return privs;
    }

    /**
     * Converts this privilege to an encoded string.
     */
    public String toEncodedString() {
        StringBuffer buf = new StringBuffer();
        buf.append("||");
        int priv_bit = 1;
        for (int i = 0; i < 11; ++i) {
            if ((privs & priv_bit) != 0) {
                buf.append(formatPriv(priv_bit));
                buf.append("||");
            }
            priv_bit = priv_bit << 1;
        }
        return new String(buf);
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        int priv_bit = 1;
        for (int i = 0; i < 11; ++i) {
            if ((privs & priv_bit) != 0) {
                buf.append(formatPriv(priv_bit));
                buf.append(' ');
            }
            priv_bit = priv_bit << 1;
        }
        return new String(buf);
    }

    public boolean equals(Object ob) {
        return privs == ((Privileges) ob).privs;
    }

    // ---------- More statics ----------

    /**
     * No privileges.
     */
    public final static Privileges EMPTY_PRIVS;

    /**
     * Enable all privs for the object.
     */
    public final static Privileges TABLE_ALL_PRIVS;

    /**
     * Read privs for the object.
     */
    public final static Privileges TABLE_READ_PRIVS;

    /**
     * All access privs for a schema object.
     */
    public final static Privileges SCHEMA_ALL_PRIVS;

    /**
     * Read access privs for a schema object.
     */
    public final static Privileges SCHEMA_READ_PRIVS;

    /**
     * All access (execute/update/delete/etc) privs for a procedure object.
     */
    public final static Privileges PROCEDURE_ALL_PRIVS;

    /**
     * Execute access privs for a procedure object.
     */
    public final static Privileges PROCEDURE_EXECUTE_PRIVS;


    static {
        Privileges p;

        EMPTY_PRIVS = new Privileges();

        p = EMPTY_PRIVS;
        p = p.add(SELECT);
        p = p.add(DELETE);
        p = p.add(UPDATE);
        p = p.add(INSERT);
        p = p.add(REFERENCES);
        p = p.add(USAGE);
        p = p.add(COMPACT);
        TABLE_ALL_PRIVS = p;

        p = EMPTY_PRIVS;
        p = p.add(SELECT);
        p = p.add(USAGE);
        TABLE_READ_PRIVS = p;

        p = EMPTY_PRIVS;
        p = p.add(CREATE);
        p = p.add(ALTER);
        p = p.add(DROP);
        p = p.add(LIST);
        SCHEMA_ALL_PRIVS = p;

        p = EMPTY_PRIVS;
        p = p.add(LIST);
        SCHEMA_READ_PRIVS = p;

        p = EMPTY_PRIVS;
        p = p.add(SELECT);
        p = p.add(DELETE);
        p = p.add(UPDATE);
        p = p.add(INSERT);
        PROCEDURE_ALL_PRIVS = p;

        p = EMPTY_PRIVS;
        p = p.add(SELECT);
        PROCEDURE_EXECUTE_PRIVS = p;

    }

}
