package com.vin.bcp;

import java.util.UUID;

import org.apache.logging.log4j.ThreadContext;

import com.vin.bcp.util.BatchConfig;

public class BulkCopyMain {

    public static void main(String[] args) {
        setLogFileName();
        BatchConfig config = new BatchConfig(args);

        BulkCopyEngine bulkCopyEngine = new BulkCopyEngine(config);
        bulkCopyEngine.run();

    }

    private static void setLogFileName() {
        String jobId = System.getenv("BCP_JOB_ID");
        if (jobId == null) {
            jobId = UUID.randomUUID().toString().toUpperCase();
        }
        ThreadContext.put("logFile", jobId);
    }
}
