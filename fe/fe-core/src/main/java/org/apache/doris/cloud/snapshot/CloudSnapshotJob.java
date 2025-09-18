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

public class CloudSnapshotJob  {

    private boolean auto;
    private long ttl; // used for manual snapshot
    private String label;

    public CloudSnapshotJob(boolean auto, long ttl, String label) {
        this.auto = auto;
        this.ttl = ttl;
        this.label = label;
    }

    public CloudSnapshotJob(boolean auto) {
        this.auto = auto;
    }

    public boolean isAuto() {
        return auto;
    }

    public long getTtl() {
        return ttl;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CloudSnapshotJob{").append("auto=").append(auto);
        if (label != null) {
            sb.append(", label=").append(label);
        }
        if (ttl > 0) {
            sb.append(", ttl=").append(ttl);
        }
        sb.append("}");
        return sb.toString();
    }
}
