package com.vin.bcp.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vin.bcp.util.DBConfig;

/**
 * Data Access Object
 * 
 */
public enum DAO {
    INSTANCE;
    private Logger logger = LogManager.getLogger(DAO.class);
    private ThreadLocal<Map<String, Object>> cache = new ThreadLocal<>();
    private static final String FETCH_PS = "fetchPS";
    private static final String WRITE_PS = "writePS";

    public int fetchData(DBConfig config,
            BlockingQueue<List<Column>> dataQueue, AtomicInteger recordsRead,
            AtomicBoolean isAborted) {
        int rowCount = 0;
        try {
            String sql = sourceSql(config);

            Map<String, Object> cached = getCached();
            PreparedStatement ps = getFetchPS(cached, FETCH_PS, config, sql);

            logger.info("Fetch SQL --> " + sql);
            logger.trace("Execute Query");
            ResultSet rs = ps.executeQuery();
            logger.trace("Execute Metadata");
            ResultSetMetaData rsmd = rs.getMetaData();

            // isAborted will convey whether the job is aborted
            // No need to read data if aborted
            while (rs.next() && !isAborted.get()) {
                rowCount++;
                List<Column> row = new ArrayList<Column>(rsmd.getColumnCount());
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    row.add(new Column(i, rs.getObject(i), rsmd
                            .getColumnType(i)));
                }
                dataQueue.put(row);
                recordsRead.incrementAndGet();
                logger.trace("Read " + rowCount + " row --> " + row);
            }

            logger.debug("Total rows = " + rowCount);
        } catch (SQLException e) {
            SQLException roote = e.getNextException();
            while (roote != null) {
                e = roote;
                roote = e.getNextException();
            }
            logger.error("Database exception occurred", e);
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            logger.error("Data queue interaction interrupted", e);
            throw new RuntimeException(e);
        }
        return rowCount;
    }

    public int writeToTarget(DBConfig config, List<List<Column>> recordset,
            AtomicBoolean isAborted) {

        try {
            Map<String, Object> cached = getCached();
            Connection target = getConnection(cached, config,
                    WRITE_PS + "Conn", false);
            String sql = targetSql(config, recordset.get(0).size());
            PreparedStatement ps = getCachedPS(cached, WRITE_PS, config, sql);
            ps.setQueryTimeout(600); // 10 minute timeout for query execution

            for (List<Column> list : recordset) {
                // In multi-threaded world, check the job abortion flag
                if (isAborted.get()) {
                    ps.clearBatch();
                    throw new RuntimeException("DB write aborted");
                }
                for (Column column : list) {
                    ps.setObject(column.position, column.value, column.type);
                }
                ps.addBatch();
            }

            int[] rows = ps.executeBatch();
            target.commit();
            ps.clearBatch();
            if (rows.length > 1) {
                return rows.length;
            } else if ((rows.length == 1) && (rows[0] > 0)) {
                return rows[0];
            } else {
                return recordset.size();
            }
        } catch (SQLException e) {
            SQLException roote = e.getNextException();
            while (roote != null) {
                e = roote;
                roote = e.getNextException();
            }
            logger.error("Database exception occurred", e);
            throw new RuntimeException(e);
        }
    }

    private String sourceSql(DBConfig config) {
        if (config.sql == null) {
            StringBuilder sql = new StringBuilder("SELECT ");
            if (null != config.columns) {
                sql.append(config.columns).append(" ");
            } else {
                sql.append("* ");
            }
            sql.append("FROM ").append(config.table);
            logger.debug("[ SOURCE SQL = " + sql + " ]");
            return sql.toString().trim();
        }
        logger.debug("[ SOURCE SQL = " + config.sql + " ]");
        return config.sql;
    }

    private String targetSql(DBConfig config, int columnCount) {
        if (config.sql == null) {

            String params = "";
            for (int i = 0; i < columnCount; i++) {
                params += "?";
                if (i < columnCount - 1) {
                    params += ",";
                }
            }

            StringBuilder sql = new StringBuilder("INSERT INTO ")
                    .append(config.table);
            if (config.columns != null) {
                sql.append(" ( ").append(config.columns).append(" ) ");
            }

            sql.append(" VALUES ( ").append(params).append(" )");
            logger.debug("[ TARGET SQL = " + sql + " ]");
            return sql.toString().trim();
        }
        logger.debug("[ TARGET SQL = " + config.sql + " ]");
        return config.sql;
    }

    private Map<String, Object> getCached() {
        Map<String, Object> cached = cache.get();
        if (cached == null) {
            logger.debug("Nothing cached. init cache");
            cached = new HashMap<>();
            cache.set(cached);
        }
        return cached;
    }

    private PreparedStatement getCachedPS(Map<String, Object> cached,
            String key, DBConfig config, String sql) throws SQLException {
        PreparedStatement ps = (PreparedStatement) cached.get(key);
        Connection conn = getConnection(cached, config, key + "Conn", false);
        if (ps == null) {

            logger.debug("Prepare new fetch sql");
            ps = conn.prepareStatement(sql);
            cached.put(key, ps);
        }
        return ps;
    }
    
    private PreparedStatement getFetchPS(Map<String, Object> cached,
            String key, DBConfig config, String sql) throws SQLException {
        logger.debug("Prepare new fetch sql");
        Connection conn = getConnection(cached, config, key + "Conn", false);
        PreparedStatement ps = conn.prepareStatement(sql);
        
        return ps;
    }

    private Connection getConnection(Map<String, Object> cached,
            DBConfig config, String key, boolean autoCommit)
            throws SQLException {
        Connection conn = (Connection) cached.get(key);

        if ((conn == null) || (!conn.isValid(config.connValidTimeout))) {
            logger.debug("Establish a new database connection");
            conn = getDBConnection(config.dbType, config.dbServerName,
                    config.dbServerPort, config.dbName, config.dbUserId,
                    config.dbPassword);
            cached.put(key, conn);
        }
        conn.setAutoCommit(autoCommit);
        return conn;
    }

    private Connection getDBConnection(String dbType, String serverName,
            String port, String database, String userName, String password) {
        if ("SYBASE".equalsIgnoreCase(dbType)) {
            return getSybaseConnection(serverName, port, database, userName,
                    password);
        } else if ("DB2".equalsIgnoreCase(dbType)) {
            return getDb2Connection(serverName, port, database, userName,
                    password);
        }
        return null;
    }

    private Connection getSybaseConnection(String serverName, String port,
            String database, String userName, String password) {
        String jdbcUrl = getSybaseJdbcUrl(serverName, port, database);
        // String driver = "com.sybase.jdbc4.jdbc.SybDriver";
        return getConnection(jdbcUrl, userName, password);
    }

    private Connection getDb2Connection(String serverName, String port,
            String database, String userName, String password) {
        String jdbcUrl = getDb2JdbcUrl(serverName, port, database);
        // String driver = "com.ibm.db2.jcc.DB2Driver";
        return getConnection(jdbcUrl, userName, password);
    }

    private String getSybaseJdbcUrl(String serverName, String port,
            String database) {
        return "jdbc:sybase:Tds:" + serverName + ":" + port + "/" + database
                + "?DYNAMIC_PREPARE=true&ENABLE_BULK_LOAD=true&CHARSET=iso_1";
    }

    private String getDb2JdbcUrl(String serverName, String port, String database) {
        return "jdbc:db2://" + serverName + ":" + port + "/" + database;
    }

    private Connection getConnection(String jdbcUrl, String userName,
            String password) {

        try {
            return DriverManager.getConnection(jdbcUrl, userName, password);
        }

        catch (SQLException e) {
            SQLException roote = e.getNextException();
            while (roote != null) {
                e = roote;
                roote = e.getNextException();
            }
            logger.error("Database exception occurred", e);
            throw new RuntimeException(e);
        }
    }
}
