// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.cloud.snapshot;

import org.apache.doris.alter.AlterJobV2.JobType;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.io.CountingDataOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

public class CloudSnapshotEnv extends CloudEnv {

    private static final Logger LOG = LogManager.getLogger(CloudSnapshotEnv.class);

    public CloudSnapshotEnv(boolean isCheckpointCatalog) {
        super(isCheckpointCatalog);
    }

    @Override
    public long saveMasterInfo(CountingDataOutputStream dos, long checksum) {
        LOG.info("skip save masterInfo");
        return checksum;
    }

    @Override
    public long saveFrontends(CountingDataOutputStream dos, long checksum) {
        LOG.info("skip save frontends");
        return checksum;
    }

    @Override
    public long saveBackends(CountingDataOutputStream dos, long checksum) {
        LOG.info("skip save backends");
        return checksum;
    }

    @Override
    public long saveAlterJob(CountingDataOutputStream dos, long checksum) {
        LOG.info("skip save alterJob");
        return checksum;
    }

    @Override
    public long saveAlterJob(CountingDataOutputStream dos, long checksum, JobType type) {
        LOG.info("skip save alterJob type: {}", type);
        return checksum;
    }

    @Override
    public long saveAnalysisMgr(CountingDataOutputStream dos, long checksum) {
        LOG.info("skip save analysisMgr");
        return checksum;
    }

    @Override
    public long saveAsyncJobManager(CountingDataOutputStream out, long checksum) {
        LOG.info("skip save asyncJobManager");
        return checksum;
    }

    @Override
    public long saveBackupHandler(CountingDataOutputStream dos, long checksum) {
        LOG.info("skip save backupHandler");
        return checksum;
    }

    @Override
    public long saveExportJob(CountingDataOutputStream dos, long checksum) {
        LOG.info("skip save exportJob");
        return checksum;
    }

    @Override
    public long saveLoadJobsV2(CountingDataOutputStream dos, long checksum) {
        LOG.info("skip save loadJobsV2");
        return checksum;
    }

    @Override
    public long saveRoutineLoadJobs(CountingDataOutputStream dos, long checksum) {
        LOG.info("skip save routineLoadJobs");
        return checksum;
    }

    public void setReplayedJournalId(long journalId) {
        this.replayedJournalId = new AtomicLong(journalId);
    }
}
