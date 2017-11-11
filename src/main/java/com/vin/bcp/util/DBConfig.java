package com.vin.bcp.util;

/**
 * Database Configuration
 * 
 */
public class DBConfig {
    /**
     * @param dbName
     * @param dbServerName
     * @param dbServerPort
     * @param dbType
     * @param dbUserId
     * @param dbPassword
     */
    public DBConfig(String dbName, String dbServerName, String dbServerPort,
            String dbType, String dbUserId, String dbPassword, String table,
            String columns, String sql) {
        this.dbName = dbName;
        this.dbServerName = dbServerName;
        this.dbServerPort = dbServerPort;
        this.dbType = dbType;
        this.dbUserId = dbUserId;
        this.dbPassword = dbPassword;
        this.columns = columns;
        this.sql = sql;
        this.table = table;
    }

    public final String dbName;
    public final String dbServerName;
    public final String dbServerPort;
    public final String dbType;
    public final String dbUserId;
    public final String dbPassword;
    public final int connValidTimeout = 30;
    public final String columns;
    public final String sql;
    public final String table;
}
