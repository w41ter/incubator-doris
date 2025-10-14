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

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.storage.RemoteBase;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CloneSnapshotState {

    @JsonProperty("from_instance_id")
    private String fromInstanceId;
    @JsonProperty("from_snapshot_id")
    private String fromSnapshotId;
    @JsonProperty("instance_id")
    private String instanceId;
    @JsonProperty("name")
    private String name;
    @JsonProperty("is_read_only")
    private Boolean isReadOnly;
    @JsonProperty("obj_info")
    private ObjInfo objInfo;
    @JsonProperty("is_succeed")
    private Boolean isSucceed;

    public static class ObjInfo {
        @JsonProperty("ak")
        private String ak;
        @JsonProperty("sk")
        private String sk;
        @JsonProperty("bucket")
        private String bucket;
        @JsonProperty("prefix")
        private String prefix;
        @JsonProperty("endpoint")
        private String endpoint;
        @JsonProperty("external_endpoint")
        private String externalEndpoint;
        @JsonProperty("region")
        private String region;
        @JsonProperty("provider")
        private String provider;

        public RemoteBase.ObjectInfo getObjInfo() {
            return new RemoteBase.ObjectInfo(getProvider(), ak, sk, bucket, endpoint, region, prefix);
        }

        public Cloud.ObjectStoreInfoPB getObjectStoreInfoPB() {
            return Cloud.ObjectStoreInfoPB.newBuilder().setAk(ak).setSk(sk).setBucket(bucket).setPrefix(prefix)
                    .setEndpoint(endpoint).setExternalEndpoint(externalEndpoint).setRegion(region)
                    .setProvider(getProvider()).build();
        }

        private Cloud.ObjectStoreInfoPB.Provider getProvider() {
            Cloud.ObjectStoreInfoPB.Provider value = Cloud.ObjectStoreInfoPB.Provider.valueOf(provider);
            if (value == null) {
                throw new IllegalArgumentException("Unknown provider: " + provider);
            }
            return value;
        }
    }

    public String getFromInstanceId() {
        return fromInstanceId;
    }

    public String getFromSnapshotId() {
        return fromSnapshotId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getName() {
        return name;
    }

    public boolean isReadOnly() {
        return isReadOnly != null && isReadOnly.booleanValue();
    }

    public boolean isSucceed() {
        return isSucceed != null && isSucceed.booleanValue();
    }

    public RemoteBase.ObjectInfo getObjInfo() {
        return objInfo.getObjInfo();
    }

    public Cloud.ObjectStoreInfoPB getObjectStoreInfoPB() {
        return objInfo.getObjectStoreInfoPB();
    }

    private void checkNotNull(String paramName, String param) {
        if (param == null || param.isEmpty()) {
            throw new IllegalArgumentException(paramName + " is null");
        }
    }

    public void check() {
        checkNotNull("from_instance_id", fromInstanceId);
        checkNotNull("from_snapshot_id", fromSnapshotId);
        checkNotNull("instance_id", instanceId);
        checkNotNull("name", name);
        if (isSucceed == null && isReadOnly == null) {
            throw new IllegalArgumentException("either set is_succeed or is_read_only");
        }
        if (isSucceed != null && isReadOnly != null && isSucceed.booleanValue()) {
            throw new IllegalArgumentException("either set is_succeed or is_read_only");
        }
        if (isSucceed != null && isSucceed.booleanValue() && objInfo != null) {
            throw new IllegalArgumentException("obj_info must be null when is_succeed is true");
        }
        if (isReadOnly != null) {
            if (isReadOnly.booleanValue()) {
                if (objInfo != null) {
                    throw new IllegalArgumentException("obj_info must be null when is_read_only is true");
                }
            } else {
                if (objInfo == null) {
                    throw new IllegalArgumentException("obj_info is null");
                }
                checkNotNull("obj_info.ak", objInfo.ak);
                checkNotNull("obj_info.sk", objInfo.sk);
                checkNotNull("obj_info.bucket", objInfo.bucket);
                // prefix can be empty
                checkNotNull("obj_info.endpoint", objInfo.endpoint);
                checkNotNull("obj_info.external_endpoint", objInfo.externalEndpoint);
                checkNotNull("obj_info.region", objInfo.region);
                checkNotNull("obj_info.provider", objInfo.provider);
            }
        }
    }
}
