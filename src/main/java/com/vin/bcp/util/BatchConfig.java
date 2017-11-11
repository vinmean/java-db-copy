package com.vin.bcp.util;


public class BatchConfig {
    private static final String SOURCE_DB_NAME = "source-db";
    private static final String SOURCE_DB_SERVER_NAME = "source-db-server";
    private static final String SOURCE_DB_SERVER_PORT = "source-db-port";
    private static final String SOURCE_TABLE_NAME = "source-table";
    private static final String SOURCE_DB_TYPE = "source-db-type";
    private static final String TARGET_DB_NAME = "target-db";
    private static final String TARGET_DB_SERVER_NAME = "target-db-server";
    private static final String TARGET_DB_SERVER_PORT = "target-db-port";
    private static final String TARGET_TABLE_NAME = "target-table";
    private static final String TARGET_DB_TYPE = "target-db-type";
    private static final String BATCH_SIZE = "batchSize";
    private static final String SOURCE_USER_ID = "source-user-id";
    private static final String SOURCE_PASSWORD = "source-password";
    private static final String TARGET_USER_ID = "target-user-id";
    private static final String TARGET_PASSWORD = "target-password";
    private static final String SOURCE_SQL_FILE = "source-sql-file";
    private static final String SOURCE_COLUMNS = "source-columns";
    private static final String TARGET_SQL_FILE = "target-sql-file";
    private static final String TARGET_COLUMNS = "target-columns";
    private static final String POOL_SIZE = "poolSize";
    private static final String CACHE_SIZE = "cacheSize";

    private static final int DEFAULT_BATCH_SIZE = 100000;

    private String sourceDBName = null;
    private String sourceDBServerName = null;
    private String sourceDBServerPort = null;
    private String sourceTable = null;
    private String sourceDBType = null;
    private String targetDBName = null;
    private String targetDBServerName = null;
    private String targetDBServerPort = null;
    private String targetTable = null;
    private String targetDBType = null;
    private String sourceUserId = null;
    private String sourcePassword = null;
    private String targetUserId = null;
    private String targetPassword = null;
    private String sourceSqlFile = null;
    private String sourceColumns = null;
    private String targetSqlFile = null;
    private String targetColumns = null;
    private int batchSize;
    private int poolSize;
    private int cacheSize;

    private DBConfig sourceDBConfig;
    private DBConfig targetDBConfig;

    public BatchConfig(String[] args) {
        if (args != null) {
            for (String s : args) {
                String[] splits = s.split("=");
                if (splits.length == 2) {
                    switch (splits[0]) {
                    case CACHE_SIZE:
                        cacheSize = Integer.parseInt(splits[1]);
                        break;
                    case POOL_SIZE:
                        poolSize = Integer.parseInt(splits[1]);
                        break;
                    case SOURCE_SQL_FILE:
                        sourceSqlFile = splits[1];
                        break;
                    case SOURCE_COLUMNS:
                        sourceColumns = splits[1];
                        break;
                    case TARGET_SQL_FILE:
                        targetSqlFile = splits[1];
                        break;
                    case TARGET_COLUMNS:
                        targetColumns = splits[1];
                        break;
                    case SOURCE_DB_NAME:
                        sourceDBName = splits[1];
                        break;
                    case SOURCE_DB_SERVER_NAME:
                        sourceDBServerName = splits[1];
                        break;
                    case SOURCE_DB_TYPE:
                        sourceDBType = splits[1];
                        break;
                    case TARGET_DB_TYPE:
                        targetDBType = splits[1];
                        break;
                    case TARGET_DB_NAME:
                        targetDBName = splits[1];
                        break;
                    case TARGET_DB_SERVER_NAME:
                        targetDBServerName = splits[1];
                        break;
                    case SOURCE_TABLE_NAME:
                        sourceTable = splits[1];
                        break;
                    case TARGET_TABLE_NAME:
                        targetTable = splits[1];
                        break;
                    case SOURCE_DB_SERVER_PORT:
                        sourceDBServerPort = splits[1];
                        break;
                    case TARGET_DB_SERVER_PORT:
                        targetDBServerPort = splits[1];
                        break;
                    case SOURCE_USER_ID:
                        sourceUserId = splits[1];
                        break;
                    case SOURCE_PASSWORD:
                        sourcePassword = splits[1];
                        break;
                    case TARGET_USER_ID:
                        targetUserId = splits[1];
                        break;
                    case TARGET_PASSWORD:
                        targetPassword = splits[1];
                        break;
                    case BATCH_SIZE:
                        batchSize = Integer.parseInt(splits[1]);
                        break;
                    default:
                        break;
                    }

                }
            }
        }
    }

    public boolean isValid() {
        return (sourceDBName != null) && (sourceDBServerName != null)
                && (sourceTable != null) && (sourceDBType != null)
                && (targetDBName != null) && (targetDBServerName != null)
                && (targetTable != null) ? true : false;
    }

    public String getSourceDBName() {
        return sourceDBName;
    }

    public String getSourceDBServerName() {
        return sourceDBServerName;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public String getTargetDBName() {
        return targetDBName;
    }

    public String getTargetDBServerName() {
        return targetDBServerName;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public String getSourceDBServerPort() {
        return sourceDBServerPort;
    }

    public String getTargetDBServerPort() {
        return targetDBServerPort;
    }

    public int getBatchSize() {
        return (batchSize > 0) ? batchSize : DEFAULT_BATCH_SIZE;
    }

    public int getPoolSize() {
        return (poolSize > 0) ? poolSize : Runtime.getRuntime()
                .availableProcessors();
    }

    /**
     * @return the cacheSize
     */
    public int getCacheSize() {
        return (cacheSize > 0) ? cacheSize : DEFAULT_BATCH_SIZE;
    }

    /**
     * @return the sourceDBType
     */
    public String getSourceDBType() {
        return sourceDBType;
    }

    /**
     * @return the targetDBType
     */
    public String getTargetDBType() {
        return targetDBType;
    }

    /**
     * @return the sourceUserId
     */
    public String getSourceUserId() {
        if (sourceUserId == null) {
            sourceUserId = System.getenv("SOURCE_USER_ID");
            if (sourceUserId == null) {
                throw new RuntimeException(
                        "Source database user id is not provided. use command line argument source-user-id or env variable SOURCE_USER_ID");
            }
        }
        return sourceUserId;
    }

    /**
     * @return the sourcePassword
     */
    public String getSourcePassword() {
        if (sourcePassword == null) {
            sourcePassword = System.getenv("SOURCE_PASSWORD");
            if (sourcePassword == null) {
                throw new RuntimeException(
                        "Source database password is not provided. use command line argument source-password or env variable SOURCE_PASSWORD");
            }
        }
        return sourcePassword;
    }

    /**
     * @return the targetUserId
     */
    public String getTargetUserId() {
        if (targetUserId == null) {
            targetUserId = System.getenv("TARGET_USER_ID");
            if (targetUserId == null) {
                throw new RuntimeException(
                        "Target database user id is not provided. use command line argument target-user-id or env variable TARGET_USER_ID");
            }
        }
        return targetUserId;
    }

    /**
     * @return the targetPassword
     */
    public String getTargetPassword() {
        if (targetPassword == null) {
            targetPassword = System.getenv("TARGET_PASSWORD");
            if (targetPassword == null) {
                throw new RuntimeException(
                        "Target database password is not provided. use command line argument target-password or env variable TARGET_PASSWORD");
            }
        }
        return targetPassword;
    }

    public String getSourceSql() {
        return (sourceSqlFile == null) ? null : FileUtil
                .readFileToString(sourceSqlFile);
    }

    public String getTargetSql() {
        return (targetSqlFile == null) ? null : FileUtil
                .readFileToString(targetSqlFile);
    }

    public String getSourceColumns() {
        return sourceColumns;
    }

    public String getTargetColumns() {
        return targetColumns;
    }

    public DBConfig getSourceDBConfig() {
        if (sourceDBConfig == null) {
            sourceDBConfig = new DBConfig(getSourceDBName(),
                    getSourceDBServerName(), getSourceDBServerPort(),
                    getSourceDBType(), getSourceUserId(), getSourcePassword(),
                    getSourceTable(), getSourceColumns(), getSourceSql());
        }
        return sourceDBConfig;
    }

    public DBConfig getTargetDBConfig() {
        if (targetDBConfig == null) {
            targetDBConfig = new DBConfig(getTargetDBName(),
                    getTargetDBServerName(), getTargetDBServerPort(),
                    getTargetDBType(), getTargetUserId(), getTargetPassword(),
                    getTargetTable(), getTargetColumns(), getTargetSql());
        }
        return targetDBConfig;
    }
}
