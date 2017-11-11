package com.vin.bcp.dao;

/**
 * Represent a database column
 *
 */
public class Column {
    public final int position;
    public final Object value;
    public final int type;

    public Column(int position, Object value, int type) {
        this.position = position;
        this.value = value;
        this.type = type;
    }
}
