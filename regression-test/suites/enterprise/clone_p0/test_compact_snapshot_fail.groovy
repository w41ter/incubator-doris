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

import groovy.json.JsonOutput
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.suite.SuiteCluster
import org.awaitility.Awaitility

suite("test_compact_snapshot_fail", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_compact_snapshot_fail because not in cloud mode")
        return
    }

    // -------------------------------------------------------------------------
    // Helper: wait for FE snapshot to reach SNAPSHOT_NORMAL state
    // -------------------------------------------------------------------------
    def wait_snapshot_completed = { cluster, snapshot_label ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(1)).atMost(java.time.Duration.ofMinutes(5)).until {
            connectWithDockerCluster(cluster) {
                def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots WHERE LABEL='${snapshot_label}'"
                logger.info("Snapshot ${snapshot_label} status: " + res.toString())
                if (res.size() == 1) {
                    def state = res[0]['STATE']
                    if (state == 'SNAPSHOT_NORMAL') {
                        return true
                    } else if (state == 'SNAPSHOT_ABORTED' || state == 'SNAPSHOT_FAILED') {
                        throw new Exception("Snapshot ${snapshot_label} failed with state: ${state}")
                    }
                }
                return false
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helper: retrieve snapshot ID from cluster_snapshots table
    // -------------------------------------------------------------------------
    def get_snapshot_id = { snapshot_label ->
        def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots WHERE LABEL='${snapshot_label}'"
        assertEquals(res.size(), 1)
        return res[0]['ID']
    }

    // -------------------------------------------------------------------------
    // Helper: call MetaService HTTP get_instance API
    // -------------------------------------------------------------------------
    def get_instance_api = { msHttpPort, instance_id, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/get_instance?token=greedisgood9999&instance_id=${instance_id}"
            check check_func
        }
    }

    // -------------------------------------------------------------------------
    // Helper: call MetaService HTTP compact_snapshot API (POST)
    // -------------------------------------------------------------------------
    def compact_snapshot_api = { msHttpPort, instance_id, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/compact_snapshot?token=greedisgood9999&instance_id=${instance_id}"
            body "{}"  // POST requires a non-null body
            check check_func
        }
    }

    // -------------------------------------------------------------------------
    // Helper: poll until snapshot_compact_status == DONE (int 2 or string name)
    // Proto JSON serialization may produce an integer or enum name string.
    // -------------------------------------------------------------------------
    def is_compact_done = { status ->
        return status != null && (status == 2 || status == 2L ||
               status.toString() == "2" || status == "SNAPSHOT_COMPACT_DONE")
    }

    def wait_compact_done = { msHttpPort, instance_id ->
        Awaitility.await()
            .pollInterval(java.time.Duration.ofSeconds(2))
            .atMost(java.time.Duration.ofMinutes(5))
            .until {
                def done = false
                get_instance_api(msHttpPort, instance_id) { respCode, body ->
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("OK"),
                        "get_instance should succeed, body=${body}")
                    def status = json.result.snapshot_compact_status
                    logger.info("Instance ${instance_id} snapshot_compact_status=${status}")
                    done = is_compact_done(status)
                }
                return done
            }
    }

    // -------------------------------------------------------------------------
    // Helper: drop a cluster's compute + SQL-server clusters, then drop instance
    // -------------------------------------------------------------------------
    def drop_cluster_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/drop_cluster?token=greedisgood9999"
            body request_body
            check check_func
        }
    }

    def drop_instance_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/drop_instance?token=greedisgood9999"
            body request_body
            check check_func
        }
    }

    def drop_instance = { msHttpPort, instance_id ->
        def jsonOutput = new JsonOutput()
        // drop compute cluster
        def clusterMap = [cluster_name: "compute_cluster", cluster_id: "compute_cluster_id"]
        def instanceMap = [instance_id: "${instance_id}", cluster: clusterMap]
        drop_cluster_api.call(msHttpPort, jsonOutput.toJson(instanceMap)) {
            respCode, body ->
                log.info("drop compute cluster resp: ${body} ${respCode}")
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }
        // drop SQL-server cluster
        clusterMap = [cluster_name: "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER",
                      cluster_id:   "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"]
        instanceMap = [instance_id: "${instance_id}", cluster: clusterMap]
        drop_cluster_api.call(msHttpPort, jsonOutput.toJson(instanceMap)) {
            respCode, body ->
                log.info("drop fe cluster resp: ${body} ${respCode}")
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }
        // drop instance
        def dropBody = jsonOutput.toJson([instance_id: "${instance_id}"])
        drop_instance_api.call(msHttpPort, dropBody) {
            respCode, body ->
                log.info("drop instance resp: ${body} ${respCode}")
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }
    }

    // =========================================================================
    // Cluster / instance layout:
    //
    //   cluster_1  (instance_id = cs_instance_1)   <- base, msNum=1
    //   cluster_2  (instance_id = cs_instance_2)   <- derived from cluster_1 snapshot
    //   cluster_3  (instance_id = cs_instance_3)   <- derived from cluster_2 snapshot
    //                                                   (forms a 3-chain for chain tests)
    //
    // Auto compact is **disabled** so we control all compact operations manually.
    // =========================================================================
    def cluster_prefix = "regression_test_compact_snapshot_"
    def cluster_1 = cluster_prefix + "cluster_1"
    def cluster_2 = cluster_prefix + "cluster_2"
    def cluster_3 = cluster_prefix + "cluster_3"

    def common_be_configs = [
        "delete_bitmap_store_write_version=2",
        "delete_bitmap_store_read_version=2",
        "delete_bitmap_store_v2_max_bytes_in_fdb=0",
    ]
    def common_ms_configs = [
        "enable_split_rowset_meta=true",
        "enable_split_tablet_schema_pb=true",
        "enable_multi_version_status=true",
        "multi_version_status_check_interval_seconds=1",
    ]

    def cluster_1_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "cs_instance_1",
        beConfigs: common_be_configs,
        msConfigs: common_ms_configs + [
            "enable_check_fe_drop_in_safe_time=false",
        ],
        recycleConfigs: [
            "recycle_interval_seconds=1",
            "recycler_sleep_before_scheduling_seconds=1",
            "enable_snapshot_data_migrator=true",
            "enable_snapshot_chain_compactor=false", // Auto compact is disabled
        ])

    def cluster_2_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 0,
        instanceId: "cs_instance_2",
        externalMsCluster: cluster_1,
        beConfigs: common_be_configs,
        msConfigs: common_ms_configs + [
            "enable_snapshot_data_migrator=true",
            "enable_snapshot_chain_compactor=false",
        ])

    def cluster_3_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 0,
        instanceId: "cs_instance_3",
        externalMsCluster: cluster_1,
        beConfigs: common_be_configs,
        msConfigs: common_ms_configs + [
            "enable_snapshot_data_migrator=true",
            "enable_snapshot_chain_compactor=false",
        ])

    def manual_init_clusters = [cluster_2, cluster_3].toSet()
    dockers(["${cluster_1}": cluster_1_opt,
             "${cluster_2}": cluster_2_opt,
             "${cluster_3}": cluster_3_opt], manual_init_clusters) { clusters ->

        // ===== Step 1: create data and snapshot in cluster_1 =================
        String snapshot_id_1 = ""
        connectWithDockerCluster(clusters[cluster_1]) {
            sql "CREATE DATABASE IF NOT EXISTS test_db"
            sql "USE test_db"
            sql """
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT,
                    name VARCHAR(100)
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            for (int i = 0; i < 5; i++) {
                sql "INSERT INTO test_table VALUES (${i}, 'data_${i}')"
            }
            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snap_1')"
            wait_snapshot_completed(clusters[cluster_1], "snap_1")
            snapshot_id_1 = get_snapshot_id("snap_1")
            logger.info("cluster_1 snapshot created: ${snapshot_id_1}")
        }

        def ms = clusters[cluster_1].getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort

        // =====================================================================
        // TestCase 1: Error – no source_instance_id (cluster_1 is a base instance)
        // Expected: the API returns a non-OK code (INVALID_ARGUMENT)
        // =====================================================================
        logger.info("=== Test Case 1: compact on non-cloned instance should fail ===")
        compact_snapshot_api(msHttpPort, "cs_instance_1") { respCode, body ->
            logger.info("compact_snapshot on base instance resp: ${body} ${respCode}")
            def json = parseJson(body)
            // Must NOT return OK for an instance without source_instance_id
            assertFalse(json.code.equalsIgnoreCase("OK"),
                "compact_snapshot on base instance (no source) should fail, but got OK")
            logger.info("Got expected error for base instance: code=${json.code} msg=${json.msg}")
        }

        // ===== Step 2: clone cluster_2 from cluster_1 snapshot ===============
        def snap_content_2 = """
        {
            "from_snapshot_id": "${snapshot_id_1}",
            "from_instance_id": "cs_instance_1",
            "instance_id": "cs_instance_2",
            "name": "cs_instance_2",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_compact_snapshot_2",
                "endpoint": "${getS3Endpoint()}",
                "external_endpoint": "${getS3Endpoint()}",
                "region": "${getS3Region()}",
                "provider": "${getS3Provider()}"
            }
        }
        """
        cluster_2_opt.clusterSnapshot = snap_content_2
        clusters[cluster_2].init(cluster_2_opt, true)

        connectWithDockerCluster(clusters[cluster_2]) {
            sql "set global enable_sql_cache = false"
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            assertEquals(res.size(), 5)
            // Insert new data in cluster_2, then create a snapshot (for 3-chain test)
            sql "INSERT INTO test_table VALUES (100, 'cluster2_data')"
            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snap_2')"
            wait_snapshot_completed(clusters[cluster_2], "snap_2")
        }

        // ===== Step 3: clone cluster_3 from cluster_2 snapshot ===============
        String snapshot_id_2 = ""
        connectWithDockerCluster(clusters[cluster_2]) {
            snapshot_id_2 = get_snapshot_id("snap_2")
            logger.info("cluster_2 snapshot created: ${snapshot_id_2}")
        }

        def snap_content_3 = """
        {
            "from_snapshot_id": "${snapshot_id_2}",
            "from_instance_id": "cs_instance_2",
            "instance_id": "cs_instance_3",
            "name": "cs_instance_3",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_compact_snapshot_3",
                "endpoint": "${getS3Endpoint()}",
                "external_endpoint": "${getS3Endpoint()}",
                "region": "${getS3Region()}",
                "provider": "${getS3Provider()}"
            }
        }
        """
        cluster_3_opt.clusterSnapshot = snap_content_3
        clusters[cluster_3].init(cluster_3_opt, true)

        // =====================================================================
        // Test Case 2: Constraint – parent not done, trying to compact child (cluster_3)
        // Chain: cluster_1 -> cluster_2 -> cluster_3
        // cluster_2 has not been compacted yet, so cluster_3 compact should fail.
        // =====================================================================
        logger.info("=== Test Case 2: compact on cluster_3 when cluster_2 not done should fail ===")
        compact_snapshot_api(msHttpPort, "cs_instance_3") { respCode, body ->
            logger.info("compact_snapshot on cluster_3 (parent not done) resp: ${body} ${respCode}")
            def json = parseJson(body)
            assertFalse(json.code.equalsIgnoreCase("OK"),
                "compact_snapshot on cluster_3 when parent not done should fail, but got OK")
            logger.info("Got expected error for cluster_3 with undone parent: code=${json.code} msg=${json.msg}")
        }

        // =====================================================================
        // Verify cluster_3 snapshot_compact_status is still UNKNOWN (0 / absent)
        // =====================================================================
        get_instance_api(msHttpPort, "cs_instance_3") { respCode, body ->
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"))
            def status = json.result.snapshot_compact_status
            // UNKNOWN = 0 (proto default, may be absent/null in JSON)
            def is_unknown = (status == null || status == 0 || status == 0L ||
                              status.toString() == "0" || status == "SNAPSHOT_COMPACT_UNKNOWN")
            assertTrue(is_unknown,
                "cluster_3 status should be UNKNOWN(0) but got: ${status}")
        }
    }
}
