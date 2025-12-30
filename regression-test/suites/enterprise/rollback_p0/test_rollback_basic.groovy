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

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.suite.SuiteCluster
import org.awaitility.Awaitility
import groovy.json.JsonOutput

suite("test_rollback_basic", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_rollback_basic because not in cloud mode")
        return
    }

    def wait_snapshot_completed = { cluster, snapshot_label ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(1)).atMost(java.time.Duration.ofMinutes(5)).until {
            connectWithDockerCluster(cluster) {
                def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots WHERE LABEL='${snapshot_label}'"
                logger.info("Snapshot ${snapshot_label} status: " + res.toString())
                return res.size() == 1 && res[0]['STATE'] != 'SNAPSHOT_PREPARE'
            }
        }
    }

    def get_snapshot_id = { snapshot_label ->
        def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots WHERE LABEL='${snapshot_label}'"
        assertEquals(res.size(), 1)
        return res[0]['ID']
    }

    def do_recycle_func = { instance_id, recyclerHttpPort ->
        def triggerRecycleBody = [instance_ids: ["${instance_id}"]]
        def jsonOutput = new JsonOutput()
        def triggerRecycleJson = jsonOutput.toJson(triggerRecycleBody)
        httpTest {
            endpoint recyclerHttpPort
            body triggerRecycleJson
            uri "/RecyclerService/http/recycle_instance?token=greedisgood9999"
        }
    }

    def do_check_func = { instance_id, recyclerHttpPort ->
        httpTest {
            op "get"
            endpoint recyclerHttpPort
            uri "/RecyclerService/http/check_instance?token=greedisgood9999&instance_id=${instance_id}"
        }
    }

    def checkerLastSuccessTime = -1
    def checkerLastFinishTime = -1
    def recyclerLastSuccessTime = -1

    def getCheckJobInfo = { instance_id, recyclerHttpPort ->
        def checkJobInfoApi = { checkFunc ->
            httpTest {
                endpoint recyclerHttpPort
                uri "/RecyclerService/http/check_job_info?token=greedisgood9999&instance_id=${instance_id}"
                op "get"
                check checkFunc
            }
        }
        checkJobInfoApi.call() {
            respCode, body ->
                logger.info("http cli result: ${body} ${respCode}")
                def checkJobInfoResult = body
                logger.info("checkJobInfoResult:${checkJobInfoResult}")
                assertEquals(respCode, 200)
                def info = parseJson(checkJobInfoResult.trim())
                if (info.last_finish_time_ms != null) { // Check done
                    checkerLastFinishTime = Long.parseLong(info.last_finish_time_ms)
                }
                if(info.last_success_time_ms != null) {
                    checkerLastSuccessTime = Long.parseLong(info.last_success_time_ms)
                }
        }
    }

    def cluster_name = "regression_test_rollback_basic"
    def opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1, recyclerNum: 1,
        instanceId: "old_instance_id",
        beConfigs: [
            "delete_bitmap_store_write_version=3",
            "delete_bitmap_store_read_version=3",
        ],
        msConfigs: [
            "enable_split_rowset_meta=true",
            "enable_split_tablet_schema_pb=true",
            "enable_multi_version_status=true",
            "multi_version_status_check_interval_seconds=1",
        ],
        recycleConfigs: [
            "recycle_interval_seconds=1",
            "check_object_interval_seconds=1",
            "recycler_sleep_before_scheduling_seconds=1",
            "enable_snapshot_data_migrator=true",
            "enable_mvcc_meta_key_check=true",
            "enable_checker=true",
            "recycle_whitelist=dummy",
        ])

    docker(opt) {
        def recyclerService = cluster.getAllRecyclers()
        def (ip, port) = recyclerService[0].getHttpAddress()
        def host = "${ip}:${port}"
        // Step 1: Create a snapshot
        String snapshot_id = ""
        connectWithDockerCluster(cluster) {
            sql "CREATE DATABASE IF NOT EXISTS test_db"
            sql "USE test_db"
            sql """
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT,
                    name VARCHAR(100)
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1");
            """
            sql "INSERT INTO test_table VALUES (1, 'cluster1_data')"
            sql "INSERT INTO test_table VALUES (2, 'cluster2_data')"
            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_label')"
            wait_snapshot_completed(cluster, "snapshot_label")
            snapshot_id = get_snapshot_id("snapshot_label")

            // Below inserts are not included in the snapshot
            sql "INSERT INTO test_table VALUES (3, 'cluster1_new_data')"
            sql "INSERT INTO test_table VALUES (4, 'cluster2_new_data')"
        }

        // Step 2: Rollback to the snapshot.
        def cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id}",
            "from_instance_id": "old_instance_id",
            "instance_id": "new_instance_id",
            "name": "new_instance",
            "is_successor": true
        }
        """
        logger.info("Rollback the cluster with snapshot: " + cluster_snapshot_content)
        cluster.rollback(cluster_snapshot_content)

        // After rollback, reconnect the cluster and check data.
        connectWithDockerCluster(cluster) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data in the cluster after rollback: " + res.toString())
            assertEquals(res.size(), 2)
            assertEquals(res[0]['id'], 1)
            assertEquals(res[0]['name'], 'cluster1_data')
            assertEquals(res[1]['id'], 2)
            assertEquals(res[1]['name'], 'cluster2_data')
        }

        sleep(10)
        def retry = 10
        do {
            do_recycle_func("old_instance_id", host)
            Thread.sleep(10000) // 10s
            do_check_func("old_instance_id", host)
            Thread.sleep(10000) // 10s
            getCheckJobInfo("old_instance_id", host)
            logger.info("checkerLastFinishTime=${checkerLastFinishTime}, checkerLastSuccessTime=${checkerLastSuccessTime}")
            if (checkerLastSuccessTime > recyclerLastSuccessTime) {
                break
            }
            retry--
        } while (retry)
        assertEquals(checkerLastFinishTime, checkerLastSuccessTime)
    }
}


