package com.vin.bcp;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vin.bcp.dao.Column;
import com.vin.bcp.dao.DAO;
import com.vin.bcp.util.BatchConfig;

public class BulkCopyEngine {
    private Logger logger = LogManager.getLogger(BulkCopyEngine.class);

    private BatchConfig config;
    private static final int MAX_ERROR = 10;

    public BulkCopyEngine(BatchConfig config) {
        this.config = config;
    }

    public void run() {
        executeBulkCopy();
    }

    private void executeBulkCopy() {
        // to keep track of job abortion
        // If dbread results in exception, isAborted is set to true
        // If db write encounters exception, then error count is increased
        // IF error count exceeds max error, then job is aborted
        AtomicBoolean isAborted = new AtomicBoolean(false);
        AtomicInteger errorCount = new AtomicInteger(0);
        try {
            DAO dao = DAO.INSTANCE;
            logger.info("Bulk copy from " + config.getSourceDBName() + "."
                    + config.getSourceTable() + " to "
                    + config.getTargetDBName() + "." + config.getTargetTable()
                    + " started");
            // the queue to hold the records read from database
            BlockingQueue<List<Column>> dataQueue = new LinkedBlockingQueue<>(
                    config.getCacheSize());

            // to keep track records written to target
            AtomicInteger recordsWritten = new AtomicInteger(0);

            // to keep track if db read is complete
            AtomicBoolean dbReadCompleted = new AtomicBoolean(false);

            // to keep track of records read
            AtomicInteger recordsRead = new AtomicInteger(0);

            // Kick off writer threads here
            ExecutorService writerPool = startWriters(dataQueue,
                    dbReadCompleted, recordsRead, recordsWritten, isAborted,
                    errorCount);

            // Read from database
            dao.fetchData(config.getSourceDBConfig(), dataQueue, recordsRead,
                    isAborted);

            // data read should now be complete
            dbReadCompleted.set(true);
            while (!writerPool.isTerminated()) {
                Thread.sleep(5000);
                // IF error count exceeds max error, then job is aborted
                if (errorCount.get() >= MAX_ERROR) {
                    isAborted.set(true);
                }
            }

            if (!isAborted.get()) {
                logger.info("Bulk copy from " + config.getSourceDBName() + "."
                        + config.getSourceTable() + " to "
                        + config.getTargetDBName() + "."
                        + config.getTargetTable() + " completed");
            } else {
                logger.warn("Bulk copy from " + config.getSourceDBName() + "."
                        + config.getSourceTable() + " to "
                        + config.getTargetDBName() + "."
                        + config.getTargetTable() + " aborted");
            }
        } catch (Exception e) {
            isAborted.set(true);
            logger.fatal(e);
        }
    }

    private ExecutorService startWriters(
            final BlockingQueue<List<Column>> dataQueue,
            final AtomicBoolean dbReadCompleted,
            final AtomicInteger recordsRead,
            final AtomicInteger recordsWritten, final AtomicBoolean isAborted,
            final AtomicInteger errorCount) {
        ExecutorService writerPool = Executors.newFixedThreadPool(
                config.getPoolSize(), new ThreadFactory() {
                    private AtomicInteger suffix = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "WRITER-"
                                + suffix.incrementAndGet());
                    }
                });

        final long startTime = System.currentTimeMillis();

        for (int i = 0; i < config.getPoolSize(); i++) {
            writerPool.execute(new Runnable() {
                private DAO dao = DAO.INSTANCE;

                @Override
                public void run() {
                    logger.info("Started writer thread");
                    int rows = 0;
                    List<List<Column>> recordset = new ArrayList<>(config
                            .getBatchSize());
                    try {
                        // the loop will run when
                        // job is not aborted AND
                        // (queue is empty but db read is not completed OR
                        // queue is not empty but db read is completed)
                        // Within the loop, commit records when batchsize is met
                        while (!isAborted.get()
                                && !(dataQueue.isEmpty() && dbReadCompleted
                                        .get())) {
                            List<Column> row = dataQueue.poll();

                            if (row != null) {
                                recordset.add(row);
                                if (recordset.size() == config.getBatchSize()) {
                                    rows = dao.writeToTarget(
                                            config.getTargetDBConfig(),
                                            recordset, isAborted);

                                    recordset.clear();
                                    logger.debug("Committed " + rows
                                            + " rows into target table "
                                            + config.getTargetTable()
                                            + " ; Total rows written = "
                                            + recordsWritten.addAndGet(rows));
                                    logger.info("[ ABORTED = "
                                            + isAborted.get()
                                            + " ] [ READ ROWS = "
                                            + recordsRead.get()
                                            + " ] [ COMMITTED ROWS = "
                                            + recordsWritten.get()
                                            + " ] [ ELAPSED TIME = "
                                            + ((System.currentTimeMillis() - startTime) / 1000.0)
                                            + " secs ]");
                                }
                            }
                        }
                        // commit the remaining records
                        if (recordset.size() > 0) {
                            rows = dao.writeToTarget(
                                    config.getTargetDBConfig(), recordset,
                                    isAborted);
                            recordset.clear();
                            logger.debug("Committed " + rows
                                    + " rows into target table "
                                    + config.getTargetTable()
                                    + " ; Total rows written = "
                                    + recordsWritten.addAndGet(rows));
                            logger.info("[ ABORTED = "
                                    + isAborted.get()
                                    + " ] [ READ ROWS = "
                                    + recordsRead.get()
                                    + " ] [ COMMITTED ROWS = "
                                    + recordsWritten.get()
                                    + " ] [ ELAPSED TIME = "
                                    + ((System.currentTimeMillis() - startTime) / 1000.0)
                                    + " secs ]");
                        }

                    } catch (Exception e) {
                        // Increase the error count if exception occurs
                        errorCount.incrementAndGet();
                        if (!recordset.isEmpty()) {
                            for (List<Column> record : recordset) {
                                // This would throw an exception if
                                // queue cannot be written
                                // Idea is to return the elements to the
                                // queue for other workers to process
                                dataQueue.add(record);
                            }
                        }
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        // No more tasks to this pool
        writerPool.shutdown();
        return writerPool;
    }

}
