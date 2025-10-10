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

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.master.Checkpoint;
import org.apache.doris.persist.EditLogFileOutputStream;
import org.apache.doris.persist.Storage;
import org.apache.doris.rpc.RpcException;

import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class CloudSnapshotHandlerImplementation extends CloudSnapshotHandler {

    private static final Logger LOG = LogManager.getLogger(CloudSnapshotHandlerImplementation.class);

    private static final String SNAPSHOT_DIR = "/snapshot/";
    private static final String CLONE_SNAPSHOT_DIR = "/clone-snapshot/";
    private String snapshotDir;
    private String cloneSnapshotDir;

    // auto snapshot job
    private CloudSnapshotJob autoSnapshotJob = null;
    private long autoSnapshotIntervalSeconds;
    private long lastFinishedAutoSnapshotTime = -1; // second
    private boolean autoSnapshotJobInitialized = false;

    // manual snapshot jobs
    private LinkedBlockingQueue<CloudSnapshotJob> manualSnapshotJobs = Queues.newLinkedBlockingQueue();

    @Override
    public void initialize() {
        this.snapshotDir = Config.meta_dir + SNAPSHOT_DIR;
        createDir(this.snapshotDir);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            getLastFinishedAutoSnapshotTime();
            if (!autoSnapshotJobInitialized) {
                refreshAutoSnapshotJob();
            }
            executeJobs();
        } catch (Throwable e) {
            LOG.warn("failed to process one round of cloud snapshot", e);
        }
    }

    @Override
    public void submitJob(long ttl, String label) throws Exception {
        CloudSnapshotJob job = new CloudSnapshotJob(false, ttl, label);
        beginSnapshotAndWriteEditLog(job);
        manualSnapshotJobs.add(job);
    }

    @Override
    public synchronized void refreshAutoSnapshotJob() {
        Cloud.GetInstanceResponse response = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudInstance();
        Cloud.InstanceInfoPB instanceInfo = response.getInstance();
        long maxReservedSnapshot = instanceInfo.hasMaxReservedSnapshot() ? instanceInfo.getMaxReservedSnapshot() : 0;
        long autoSnapshotIntervalSeconds = instanceInfo.hasSnapshotIntervalSeconds()
                ? instanceInfo.getSnapshotIntervalSeconds() : 3600;
        if (instanceInfo.hasSnapshotSwitchStatus()
                && instanceInfo.getSnapshotSwitchStatus() == Cloud.SnapshotSwitchStatus.SNAPSHOT_SWITCH_ON
                && maxReservedSnapshot > 0) {
            if (this.autoSnapshotJob == null) {
                this.autoSnapshotJob = new CloudSnapshotJob(true);
            }
            this.autoSnapshotIntervalSeconds = autoSnapshotIntervalSeconds;
        } else {
            this.autoSnapshotJob = null;
        }
        autoSnapshotJobInitialized = true;
        LOG.info("auto snapshot job is {}, interval: {}", this.autoSnapshotJob != null ? "ON" : "OFF",
                this.autoSnapshotIntervalSeconds);
    }

    private void getLastFinishedAutoSnapshotTime() {
        if (lastFinishedAutoSnapshotTime >= 0) {
            return;
        }
        try {
            Cloud.ListSnapshotResponse response = listSnapshot(false);
            for (Cloud.SnapshotInfoPB snapshotInfoPB : response.getSnapshotsList()) {
                if (!snapshotInfoPB.getAutoSnapshot()) {
                    continue;
                }
                if (snapshotInfoPB.getFinishAt() > lastFinishedAutoSnapshotTime) {
                    lastFinishedAutoSnapshotTime = snapshotInfoPB.getFinishAt();
                }
            }
            if (lastFinishedAutoSnapshotTime == -1) {
                lastFinishedAutoSnapshotTime = 0;
            }
            LOG.info("lastFinishedAutoSnapshotTime: {}", lastFinishedAutoSnapshotTime);
        } catch (DdlException e) {
            LOG.warn("failed to list snapshot", e);
        }
    }

    private void executeJobs() {
        while (true) {
            if (manualSnapshotJobs.isEmpty()) {
                break;
            }
            CloudSnapshotJob job = manualSnapshotJobs.poll();
            try {
                executeJob(job);
            } catch (Exception e) {
                LOG.warn("manual snapshot job failed: {}", job, e);
            }
        }
        if (autoSnapshotJob != null && lastFinishedAutoSnapshotTime + autoSnapshotIntervalSeconds
                < System.currentTimeMillis() / 1000) {
            try {
                String label = "auto_snapshot_" + System.currentTimeMillis();
                autoSnapshotJob.setLabel(label);
                executeJob(autoSnapshotJob);
            } catch (Exception e) {
                LOG.warn("auto snapshot job failed: {}", autoSnapshotJob, e);
            } finally {
                autoSnapshotJob.setLabel(null);
            }
        }
    }

    private void beginSnapshotAndWriteEditLog(CloudSnapshotJob job) throws Exception {
        synchronized (Env.getCurrentEnv().getEditLog()) {
            // begin snapshot
            Cloud.BeginSnapshotResponse response = beginSnapshot(job);
            job.setBeginSnapshotResponse(response);
            // write edit log
            SnapshotState snapshotState = new SnapshotState(response.getSnapshotId(), response.getImageUrl());
            long logId = Env.getCurrentEnv().getEditLog().logBeginSnapshot(snapshotState);
            job.setLogId(logId);
        }
    }

    private void executeJob(CloudSnapshotJob job) {
        String snapshotId = null;
        long logId = 0;
        try {
            LOG.info("start to snapshot for job: {}", job);
            // 1. begin snapshot and write edit log
            if (job.isAuto()) {
                beginSnapshotAndWriteEditLog(job);
            }
            if (job.getBeginSnapshotResponse() == null) {
                throw new DdlException("snapshot failed because begin snapshot response is null");
            }
            if (job.getLogId() == 0) {
                throw new DdlException("snapshot failed because log id is 0");
            }
            Cloud.BeginSnapshotResponse beginSnapshotResponse = job.getBeginSnapshotResponse();
            snapshotId = beginSnapshotResponse.getSnapshotId();
            String imageUrl = beginSnapshotResponse.getImageUrl();
            Cloud.ObjectStoreInfoPB objInfo = beginSnapshotResponse.getObjInfo();
            logId = job.getLogId();
            // 2. upload image
            Checkpoint checkpoint = Env.getCurrentEnv().getCheckpointer();
            checkpoint.getLock().readLock().lock();
            try {
                uploadImage(snapshotId, imageUrl, objInfo, logId);
            } finally {
                checkpoint.getLock().readLock().unlock();
            }
            // 3. commit snapshot
            commitSnapshot(snapshotId, imageUrl, logId);
            if (job.isAuto()) {
                lastFinishedAutoSnapshotTime = System.currentTimeMillis() / 1000;
            }
            LOG.info("succeed to snapshot for job: {}, id: {}, imageUrl: {}, logId: {}", job, snapshotId, imageUrl,
                    logId);
        } catch (Exception e) {
            LOG.warn("failed to snapshot for job: {}", job, e);
            // abort snapshot
            try {
                if (snapshotId != null) {
                    abortSnapshot(snapshotId, e.getMessage());
                }
            } catch (Exception e1) {
                LOG.warn("failed to abort snapshot for job: {}", job, e1);
            }
            // delete edit log file and zip file
            File[] files = {getEditLogFile(logId), getImageZipFile(snapshotId)};
            for (File file : files) {
                try {
                    if (file.exists()) {
                        file.delete();
                        LOG.info("delete file: {}", file.getAbsolutePath());
                    }
                } catch (Exception e1) {
                    LOG.warn("failed to delete file: {} for job: {}", file.getAbsolutePath(), job, e1);
                }
            }
        } finally {
            job.setBeginSnapshotResponse(null);
            job.setLogId(0);
        }
    }

    private Cloud.BeginSnapshotResponse beginSnapshot(CloudSnapshotJob job) throws Exception {
        Cloud.BeginSnapshotRequest.Builder builder = Cloud.BeginSnapshotRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id).setTimeoutSeconds(Config.cloud_snapshot_timeout_seconds)
                .setAutoSnapshot(job.isAuto());
        if (job.getTtl() > 0) {
            builder.setTtlSeconds(job.getTtl());
        }
        if (job.getLabel() != null) {
            builder.setSnapshotLabel(job.getLabel());
        }
        try {
            Cloud.BeginSnapshotResponse response = MetaServiceProxy.getInstance().beginSnapshot(builder.build());
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("beginSnapshot response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
            return response;
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private void commitSnapshot(String snapshotId, String imageUrl, long logId) throws Exception {
        try {
            Cloud.CommitSnapshotRequest request = Cloud.CommitSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id).setSnapshotId(snapshotId).setImageUrl(imageUrl)
                    .setLastJournalId(logId).build();
            Cloud.CommitSnapshotResponse response = MetaServiceProxy.getInstance().commitSnapshot(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("commitSnapshot response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private Pair<Boolean, String> updateSnapshotUploadId(String snapshotId, String uploadFile, String uploadId) {
        try {
            Cloud.UpdateSnapshotRequest request = Cloud.UpdateSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id).setSnapshotId(snapshotId).setUploadFile(uploadFile)
                    .setUploadId(uploadId).build();
            Cloud.UpdateSnapshotResponse response = MetaServiceProxy.getInstance().updateSnapshot(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("updateSnapshot response: {} ", response);
                return Pair.of(false, response.getStatus().getMsg());
            }
            return Pair.of(true, null);
        } catch (RpcException e) {
            LOG.warn("failed to update snapshot for snapshotId: {}, fileName: {}, uploadId: {}",
                    snapshotId, uploadFile, uploadId, e);
            return Pair.of(false, e.getMessage());
        }
    }

    private void uploadImage(String snapshotId, String imageUrl, Cloud.ObjectStoreInfoPB objInfo,
            long logId) throws Exception {
        LOG.info("start to snapshot for id: {}, imageUrl: {}, logId: {}", snapshotId, imageUrl, logId);
        List<File> files = new ArrayList<>();

        // 1. check image file exist
        long imageVersion = getImageVersion();
        if (imageVersion > logId) {
            throw new DdlException("image version " + imageVersion + " is larger than log id " + logId);
        }
        if (imageVersion > 0) {
            String imageDir = Env.getServingEnv().getImageDir();
            String imageFileName = "image." + imageVersion;
            File imageFile = new File(imageDir + "/" + imageFileName);
            if (!imageFile.exists()) {
                LOG.error("image file does not exist: {}", imageFile.getAbsoluteFile());
                throw new DdlException("image file does not exist: " + imageFile.getAbsoluteFile());
            }
            files.add(imageFile);
        }

        // 2. scan edit logs between [imageVersion + 1, logId], write edit log file
        File snapshotEditLogFile = null;
        if (imageVersion + 1 < logId) {
            snapshotEditLogFile = writeSnapshotEditLogFile(imageVersion + 1, logId, snapshotId);
            files.add(snapshotEditLogFile);
        }

        // 3. compress files
        File zipFile = getImageZipFile(snapshotId);
        compressFiles(files, zipFile);
        if (!zipFile.exists()) {
            throw new DdlException("zip file does not exist: " + zipFile.getAbsoluteFile());
        }

        // 4, upload zip file
        RemoteBase remote = RemoteBase.newInstance(new RemoteBase.ObjectInfo(objInfo));
        try {
            remote.multipartUploadObject(zipFile, formatRemoteKey(objInfo.getPrefix(), imageUrl, zipFile.getName()),
                    (Function<String, Pair<Boolean, String>>) uploadId -> updateSnapshotUploadId(snapshotId,
                            zipFile.getName(), uploadId));
        } finally {
            remote.close();
        }

        // 5. delete edit log file and zip file
        if (snapshotEditLogFile != null) {
            snapshotEditLogFile.delete();
        }
        zipFile.delete();
    }

    private File getEditLogFile(long logId) {
        return new File(this.snapshotDir, "edits." + logId);
    }

    private File getImageZipFile(String snapshotId) {
        return new File(this.snapshotDir, snapshotId + ".zip");
    }

    private long getImageVersion() throws DdlException {
        try {
            Storage storage = new Storage(Env.getServingEnv().getImageDir());
            return storage.getLatestImageSeq();
        } catch (Throwable e) {
            LOG.warn("get image version failed", e);
            throw new DdlException("get image version failed: " + e.getMessage());
        }
    }

    private File writeSnapshotEditLogFile(long fromJournalId, long toJournalId, String snapshotId) throws Exception {
        LOG.info("scan journal from {} to {} for snapshotId: {}", fromJournalId, toJournalId, snapshotId);
        JournalCursor cursor = Env.getCurrentEnv().getEditLog().getJournal().read(fromJournalId, toJournalId, false);
        if (cursor == null) {
            LOG.warn("failed to get cursor from {} to {}", fromJournalId, toJournalId);
            throw new DdlException("failed to get cursor from " + fromJournalId + " to " + toJournalId);
        }

        File snapshotEditLogFile = getEditLogFile(toJournalId);
        if (snapshotEditLogFile.exists()) {
            snapshotEditLogFile.delete();
        }
        if (!snapshotEditLogFile.createNewFile()) {
            LOG.warn("failed to create snapshot edits log file {}", snapshotEditLogFile.getAbsolutePath());
            throw new Exception("failed to create snapshot edits log file " + snapshotEditLogFile.getAbsolutePath());
        }
        EditLogFileOutputStream outputStream = null;
        try {
            outputStream = new EditLogFileOutputStream(snapshotEditLogFile);
            while (true) {
                Pair<Long, JournalEntity> kv = cursor.next();
                if (kv == null) {
                    break;
                }
                JournalEntity entity = kv.second;
                if (entity == null) {
                    break;
                }
                outputStream.write(entity.getOpCode(), entity.getData());
            }
            outputStream.setReadyToFlush();
            outputStream.flush();
            outputStream.close();
            return snapshotEditLogFile;
        } catch (Exception e) {
            LOG.warn("write snapshot edit log failed for id: {}", snapshotId, e);
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException ex) {
                    LOG.warn("failed to close output stream for id: {}", snapshotId, ex);
                }
            }
            try {
                if (snapshotEditLogFile.exists()) {
                    snapshotEditLogFile.delete();
                }
            } catch (Exception ex) {
                LOG.warn("failed to delete snapshot file for id: {}", snapshotId, ex);
            }
            throw new Exception(e.getMessage());
        }
    }

    private void abortSnapshot(String snapshotId, String reason) throws Exception {
        try {
            Cloud.AbortSnapshotRequest request = Cloud.AbortSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id).setSnapshotId(snapshotId).setReason(reason).build();
            Cloud.AbortSnapshotResponse response = MetaServiceProxy.getInstance().abortSnapshot(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("abortSnapshot response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private void createDir(String dir) {
        truncateDir(dir);
        File directory = new File(dir);
        if (!directory.mkdir()) {
            LOG.error("failed to create directory: {}", directory.getAbsolutePath());
        }
    }

    private void truncateDir(String dir) {
        File directory = new File(dir);
        if (directory.exists()) {
            if (directory.isDirectory()) {
                for (File file : directory.listFiles()) {
                    if (!file.delete()) {
                        LOG.warn("failed to delete file: {}", file.getAbsolutePath());
                    } else {
                        LOG.info("delete file: {}", file.getAbsolutePath());
                    }
                }
            }
            if (!directory.delete()) {
                LOG.warn("failed to delete directory: {}", directory.getAbsolutePath());
            } else {
                LOG.info("delete directory: {}", directory.getAbsolutePath());
            }
        }
    }

    private String formatRemoteKey(String prefix, String imageUrl, String fileName) {
        String newPrefix = prefix;
        if (prefix.endsWith("/")) {
            newPrefix = prefix.substring(0, prefix.length() - 1);
        }
        String newImageUrl = imageUrl;
        if (!newImageUrl.startsWith("/")) {
            newImageUrl = "/" + newImageUrl;
        }
        if (!newImageUrl.endsWith("/")) {
            newImageUrl = newImageUrl + "/";
        }
        return newPrefix + newImageUrl + fileName;
    }

    private void compressFiles(List<File> sourceFiles, File zipFile) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(zipFile);
                ZipOutputStream zos = new ZipOutputStream(fos)) {
            for (File fileToZip : sourceFiles) {
                if (!fileToZip.exists()) {
                    throw new IOException("source file does not exist: " + fileToZip.getAbsolutePath());
                }
                try (FileInputStream fis = new FileInputStream(fileToZip)) {
                    ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
                    zos.putNextEntry(zipEntry);

                    byte[] buffer = new byte[1024];
                    int length;
                    while ((length = fis.read(buffer)) > 0) {
                        zos.write(buffer, 0, length);
                    }
                    zos.closeEntry();
                }
            }
        }
    }

    private void decompressZip(File zipFile, String destDir) throws IOException {
        File dir = new File(destDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        try (FileInputStream fis = new FileInputStream(zipFile);
                ZipInputStream zis = new ZipInputStream(fis)) {
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                String filePath = destDir + File.separator + zipEntry.getName();
                if (!zipEntry.isDirectory()) {
                    extractFile(zis, filePath);
                } else {
                    File dirToCreate = new File(filePath);
                    dirToCreate.mkdirs();
                }
                zis.closeEntry();
                zipEntry = zis.getNextEntry();
            }
        }
    }

    private void extractFile(ZipInputStream zis, String filePath) throws IOException {
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath))) {
            byte[] buffer = new byte[1024];
            int read;
            while ((read = zis.read(buffer)) != -1) {
                bos.write(buffer, 0, read);
            }
        }
    }
}
