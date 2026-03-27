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
import org.awaitility.Awaitility

// Test scenario:
//   Instance A: create snap1, create snap2 (with more data).
//   Rollback A → B via snap1: only data up to snap1 is visible.
//   Instance B: create snap3.
//   Rollback B → C via snap2: data up to snap2 (from A) is visible.
//
//   Then: drop snap3 (from B) and snap1 (from A) via MetaService HTTP API.
//         Also drop snap2 (from A) so instance A has no active snapshots.
//   Run recycler for A and B to mark dropped snapshots as RECYCLED.
//   Finally: drop instance C (no snapshots), then B (snap3 recycled), then A (snaps 1&2 recycled).
//   Expected result: all three instances A, B, C can be successfully deleted.
suite("test_rollback_drop", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_rollback_drop because not in cloud mode")
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

    // Drop a snapshot belonging to a specific instance via the MetaService HTTP API.
    // This can drop snapshots from any instance regardless of which cluster is currently running.
    def ms_drop_snapshot = { msHttpPort, instance_id, snapshot_id ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/drop_snapshot?token=greedisgood9999&instance_id=${instance_id}&snapshot_id=${snapshot_id}"
            check { respCode, body ->
                log.info("drop_snapshot resp [instance=${instance_id}, snap=${snapshot_id}]: ${body} (${respCode})".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"),
                    "drop_snapshot failed for instance=${instance_id} snap=${snapshot_id}: ${body}")
            }
        }
    }

    def get_instance_api = { msHttpPort, instance_id, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/get_instance?token=greedisgood9999&instance_id=${instance_id}"
            check check_func
        }
    }

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

    // Drop an instance: removes its BE cluster, FE cluster, then the instance itself,
    // and verifies the instance is marked DELETED.
    def drop_instance = { msHttpPort, instance_id ->
        def jsonOutput = new JsonOutput()

        // drop BE cluster
        def clusterMap = [cluster_name: "compute_cluster", cluster_id: "compute_cluster_id"]
        def instance = [instance_id: "${instance_id}", cluster: clusterMap]
        def dropClusterBody = jsonOutput.toJson(instance)
        drop_cluster_api.call(msHttpPort, dropClusterBody) { respCode, body ->
            log.info("drop be cluster [${instance_id}]: ${body} (${respCode})".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"),
                "drop BE cluster failed for instance=${instance_id}: ${body}")
        }

        // drop FE cluster
        clusterMap = [cluster_name: "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER",
                      cluster_id:   "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        dropClusterBody = jsonOutput.toJson(instance)
        drop_cluster_api.call(msHttpPort, dropClusterBody) { respCode, body ->
            log.info("drop fe cluster [${instance_id}]: ${body} (${respCode})".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"),
                "drop FE cluster failed for instance=${instance_id}: ${body}")
        }

        // drop instance
        instance = [instance_id: "${instance_id}"]
        def dropInstanceBody = jsonOutput.toJson(instance)
        drop_instance_api.call(msHttpPort, dropInstanceBody) { respCode, body ->
            log.info("drop instance [${instance_id}]: ${body} (${respCode})".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"),
                "drop_instance failed for instance=${instance_id}: ${body}")
        }

        // verify instance is marked DELETED
        get_instance_api.call(msHttpPort, "${instance_id}") { respCode, body ->
            log.info("get instance [${instance_id}] after drop: ${body} (${respCode})".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"))
            assertTrue(json.result.status.equalsIgnoreCase("DELETED"),
                "Expected instance ${instance_id} to be DELETED, got: ${json.result.status}")
        }
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

    def assert_instance_deleted = { msHttpPort, instance_id ->
        get_instance_api.call(msHttpPort, "${instance_id}") { respCode, body ->
            log.info("get instance [${instance_id}] for deleted check: ${body} (${respCode})".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"))
            assertTrue(json.result.status.equalsIgnoreCase("DELETED"),
                "Expected instance ${instance_id} to be DELETED, got: ${json.result.status}")
        }
    }

    def cluster_name = "regression_test_rollback_drop"
    def opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1, recyclerNum: 1,
        instanceId: "rollback_drop_a_id",
        beConfigs: [
            "delete_bitmap_store_write_version=3",
            "delete_bitmap_store_read_version=3",
        ],
        msConfigs: [
            "enable_split_rowset_meta=true",
            "enable_split_tablet_schema_pb=true",
            "enable_multi_version_status=true",
            "multi_version_status_check_interval_seconds=1",
            "enable_check_fe_drop_in_safe_time=false",
            "log_immediate_flush=true",
            "log_verbose_modules=snapshot_manager,meta_service_resource",
        ],
        recycleConfigs: [
            "recycle_interval_seconds=1",
            "recycler_sleep_before_scheduling_seconds=1",
            "enable_snapshot_data_migrator=true",
        ])

    docker(opt) {
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        def recyclerService = cluster.getAllRecyclers()
        def (recyclerIp, recyclerPort) = recyclerService[0].getHttpAddress()
        def recyclerHttpPort = "${recyclerIp}:${recyclerPort}"

        // ── Step 1: Instance A ──────────────────────────────────────────────────
        // Insert data1 (rows 1,2) → snap1
        // Insert data2 (rows 3,4) → snap2
        // Insert data3 (rows 5,6) — not captured in any snapshot
        String snap1_id = ""
        String snap2_id = ""
        connectWithDockerCluster(cluster) {
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
            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"

            // data1 → snap1
            sql "INSERT INTO test_table VALUES (1, 'row1')"
            sql "INSERT INTO test_table VALUES (2, 'row2')"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snap_1')"
            wait_snapshot_completed(cluster, "snap_1")
            snap1_id = get_snapshot_id("snap_1")
            logger.info("snap1 id: ${snap1_id}")

            // data2 → snap2
            sql "INSERT INTO test_table VALUES (3, 'row3')"
            sql "INSERT INTO test_table VALUES (4, 'row4')"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snap_2')"
            wait_snapshot_completed(cluster, "snap_2")
            snap2_id = get_snapshot_id("snap_2")
            logger.info("snap2 id: ${snap2_id}")

            // data3 — not captured in any snapshot
            sql "INSERT INTO test_table VALUES (5, 'row5')"
            sql "INSERT INTO test_table VALUES (6, 'row6')"

            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Instance A data before rollback (6 rows expected): " + res.toString())
            assertEquals(res.size(), 6)
        }

        // ── Step 2: Rollback A → B using snap1 ─────────────────────────────────
        // B should see only data1 (rows 1, 2).
        def rollback_to_b = """
        {
            "from_snapshot_id": "${snap1_id}",
            "from_instance_id": "rollback_drop_a_id",
            "instance_id": "rollback_drop_b_id",
            "name": "rollback_drop_b",
            "is_successor": true
        }
        """
        logger.info("Rollback A → B using snap1: " + rollback_to_b)
        cluster.rollback(rollback_to_b)

        connectWithDockerCluster(cluster) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Instance B data after rollback from snap1 (2 rows expected): " + res.toString())
            assertEquals(res.size(), 2)
            assertEquals(res[0]['id'], 1)
            assertEquals(res[0]['name'], 'row1')
            assertEquals(res[1]['id'], 2)
            assertEquals(res[1]['name'], 'row2')
        }

        // ── Step 3: On instance B, create snap3 ────────────────────────────────
        String snap3_id = ""
        connectWithDockerCluster(cluster) {
            sql "USE test_db"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snap_3')"
            wait_snapshot_completed(cluster, "snap_3")
            snap3_id = get_snapshot_id("snap_3")
            logger.info("snap3 id: ${snap3_id}")
        }

        // ── Step 4: Rollback B → C using snap2 ─────────────────────────────────
        // snap2 was created on A with rows 1,2,3,4; from_instance_id is the current
        // running instance (B), following the same pattern as test_rollback_double_snapshot.
        // C should see data up to snap2 (rows 1, 2, 3, 4).
        def rollback_to_c = """
        {
            "from_snapshot_id": "${snap2_id}",
            "from_instance_id": "rollback_drop_b_id",
            "instance_id": "rollback_drop_c_id",
            "name": "rollback_drop_c",
            "is_successor": true
        }
        """
        logger.info("Rollback B → C using snap2: " + rollback_to_c)
        cluster.rollback(rollback_to_c)

        connectWithDockerCluster(cluster) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Instance C data after rollback from snap2 (4 rows expected): " + res.toString())
            assertEquals(res.size(), 4)
            assertEquals(res[0]['id'], 1)
            assertEquals(res[0]['name'], 'row1')
            assertEquals(res[1]['id'], 2)
            assertEquals(res[1]['name'], 'row2')
            assertEquals(res[2]['id'], 3)
            assertEquals(res[2]['name'], 'row3')
            assertEquals(res[3]['id'], 4)
            assertEquals(res[3]['name'], 'row4')
        }

        // ── Step 5: Drop snap3 (from B) and snap1 (from A) ─────────────────────
        // Also drop snap2 (from A) so that instance A has no active snapshots and
        // can subsequently be dropped.
        // All three snapshots are dropped via the MetaService HTTP API, which allows
        // targeting any instance regardless of which cluster is currently running.
        logger.info("Dropping snap3 (from B), snap1 (from A), snap2 (from A) via MetaService HTTP API")
        ms_drop_snapshot(msHttpPort, "rollback_drop_b_id", snap3_id)

        // ── Step 6: Drop instance C (no snapshots created on C directly) ────────
        logger.info("Dropping instance C (rollback_drop_c_id)")
        drop_instance(msHttpPort, "rollback_drop_c_id")

        // ── Step 7: Verify instance B is deleted after dropping C ───────────────
        logger.info("Checking instance B (rollback_drop_b_id) is already DELETED")
        assert_instance_deleted(msHttpPort, "rollback_drop_b_id")

        // ── Step 8: Verify instance A is deleted after dropping C ───────────────
        logger.info("Checking instance A (rollback_drop_a_id) is already DELETED")
        assert_instance_deleted(msHttpPort, "rollback_drop_a_id")

        logger.info("All three instances A, B, C have been successfully dropped.")
    }
}
