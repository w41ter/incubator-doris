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

// Regression test for the "double rollback data loss" bug:
//   1. On orig_instance: write data1, take snapshot_t1, write data2, take snapshot_t2, write data3 (no snapshot).
//   2. Rollback orig_instance → rollback1_instance (from snapshot_t1): only data1 visible. ✓
//   3. Rollback rollback1_instance → rollback2_instance (from snapshot_t2, from_instance_id=rollback1_instance_id):
//      EXPECT data1 + data2 visible.  BUG: 0 rows returned and SHOW DATA = 0.
suite("test_rollback_double_snapshot", "snapshot,docker") {
    if (!isCloudMode()) {
        logger.info("Skip test_rollback_double_snapshot because not in cloud mode")
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

    def compact_snapshot = { msHttpPort, instance_id ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/compact_snapshot?token=greedisgood9999&instance_id=${instance_id}"
            check { respCode, body ->
                log.info("compact_snapshot resp for ${instance_id}: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"),
                    "compact_snapshot failed for instance_id=${instance_id}: ${body}")
            }
        }
    }

    // Poll get_instance until snapshot_compact_status == "SNAPSHOT_COMPACT_DONE".
    // compact_snapshot is handled asynchronously by the recycler's chain compactor.
    def wait_compact_done = { msHttpPort, instance_id ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(2)).atMost(java.time.Duration.ofMinutes(5)).until {
            def done = false
            get_instance_api.call(msHttpPort, instance_id) { respCode, body ->
                log.info("wait_compact_done get_instance resp for ${instance_id}: ${body}".toString())
                def json = parseJson(body)
                if (json.code.equalsIgnoreCase("OK") && json.result != null) {
                    done = (json.result.snapshot_compact_status == "SNAPSHOT_COMPACT_DONE")
                }
            }
            return done
        }
        logger.info("compact done for instance_id=${instance_id}")
    }

    def decouple_instance_api = { msHttpPort, instance_id ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/decouple_instance?token=greedisgood9999&instance_id=${instance_id}"
            check { respCode, body ->
                log.info("decouple_instance resp for ${instance_id}: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"),
                    "decouple_instance failed for instance_id=${instance_id}: ${body}")
            }
        }
    }

    def drop_instance = { msHttpPort, instance_id ->
        def jsonOutput = new JsonOutput()

        // drop be cluster
        def clusterMap = [cluster_name: "compute_cluster", cluster_id: "compute_cluster_id"]
        def instance = [instance_id: "${instance_id}", cluster: clusterMap]
        def dropClusterBody = jsonOutput.toJson(instance)
        drop_cluster_api.call(msHttpPort, dropClusterBody) { respCode, body ->
            log.info("drop be cluster http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // drop fe cluster
        clusterMap = [cluster_name: "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER", cluster_id: "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        dropClusterBody = jsonOutput.toJson(instance)
        drop_cluster_api.call(msHttpPort, dropClusterBody) { respCode, body ->
            log.info("drop fe cluster http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // drop instance
        instance = [instance_id: "${instance_id}"]
        def dropInstanceBody = jsonOutput.toJson(instance)
        drop_instance_api.call(msHttpPort, dropInstanceBody) { respCode, body ->
            log.info("drop instance http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // verify instance is marked DELETED
        get_instance_api.call(msHttpPort, "${instance_id}") { respCode, body ->
            log.info("get instance resp after drop: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"))
            assertTrue(json.result.status.equalsIgnoreCase("DELETED"))
        }
    }

    def cluster_name = "regression_test_rollback_double_snapshot"

    def opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1, recyclerNum: 1,
        instanceId: "orig_instance_id",
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
        ],
        recycleConfigs: [
            "recycle_interval_seconds=1",
            "recycler_sleep_before_scheduling_seconds=1",
            "enable_snapshot_data_migrator=true",
            "enable_snapshot_chain_compactor=true",
            "log_verbose_modules=snapshot_chain_compactor"
        ])

    docker(opt) {
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort

        // Step 1: On orig_instance — write data1 → snapshot_t1 → write data2 → snapshot_t2 → write data3
        String snapshot_id_t1 = ""
        String snapshot_id_t2 = ""
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
            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"

            // Write data1, then take snapshot_t1
            sql "INSERT INTO test_table VALUES (1, 'data1_a')"
            sql "INSERT INTO test_table VALUES (2, 'data1_b')"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_t1')"
            wait_snapshot_completed(cluster, "snapshot_t1")
            snapshot_id_t1 = get_snapshot_id("snapshot_t1")
            logger.info("snapshot_t1 id: ${snapshot_id_t1}")

            // Write data2, then take snapshot_t2
            sql "INSERT INTO test_table VALUES (3, 'data2_a')"
            sql "INSERT INTO test_table VALUES (4, 'data2_b')"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_t2')"
            wait_snapshot_completed(cluster, "snapshot_t2")
            snapshot_id_t2 = get_snapshot_id("snapshot_t2")
            logger.info("snapshot_t2 id: ${snapshot_id_t2}")

            // Write data3: not captured in any snapshot
            sql "INSERT INTO test_table VALUES (5, 'data3_a')"
            sql "INSERT INTO test_table VALUES (6, 'data3_b')"

            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("orig_instance data before any rollback (6 rows expected): " + res.toString())
            assertEquals(res.size(), 6)
        }

        // Step 2: First rollback — orig_instance → rollback1_instance (from snapshot_t1)
        //   Expected: only data1 (rows 1, 2) is visible.
        def snapshot_content_to_t1 = """
        {
            "from_snapshot_id": "${snapshot_id_t1}",
            "from_instance_id": "orig_instance_id",
            "instance_id": "rollback1_instance_id",
            "name": "rollback1_instance",
            "is_successor": true
        }
        """
        logger.info("First rollback to snapshot_t1: " + snapshot_content_to_t1)
        cluster.rollback(snapshot_content_to_t1)

        connectWithDockerCluster(cluster) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data after first rollback to t1 (2 rows expected): " + res.toString())
            assertEquals(res.size(), 2)
            assertEquals(res[0]['id'], 1)
            assertEquals(res[0]['name'], 'data1_a')
            assertEquals(res[1]['id'], 2)
            assertEquals(res[1]['name'], 'data1_b')

            def showData = sql_return_maparray "SHOW DATA"
            logger.info("SHOW DATA after first rollback: " + showData.toString())
            def tableRow = showData.find { it['TableName'] == 'test_table' }
            assertNotNull(tableRow, "test_table must appear in SHOW DATA after first rollback")
            assertNotEquals(tableRow['Size'], '0.000 B',
                "SHOW DATA size must not be 0 for test_table after rollback to snapshot_t1")
        }

        // rollback1_instance_id was created via rollback and references orig_instance's snapshot_t1 data.
        // Compact it first (migrates owned rowsets to its own storage), then wait for completion,
        // then decouple it from the snapshot chain before dropping orig_instance.
        compact_snapshot(msHttpPort, "rollback1_instance_id")
        wait_compact_done(msHttpPort, "rollback1_instance_id")
        decouple_instance_api(msHttpPort, "rollback1_instance_id")

        // Verify data is still accessible after orig_instance is dropped.
        connectWithDockerCluster(cluster) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data in rollback1_instance after dropping orig_instance_id (2 rows expected): " + res.toString())
            assertEquals(res.size(), 2)
            def showData = sql_return_maparray "SHOW DATA"
            logger.info("SHOW DATA after dropping orig_instance_id: " + showData.toString())
            def tableRow = showData.find { it['TableName'] == 'test_table' }
            assertNotNull(tableRow)
            assertNotEquals(tableRow['Size'], '0.000 B',
                "SHOW DATA size must not be 0 after orig_instance is dropped (snapshot_t1 still in use)")
        }

        // Step 3: Second rollback — rollback1_instance → rollback2_instance (from snapshot_t2)
        //   from_instance_id is the CURRENT running instance (rollback1_instance_id).
        //   snapshot_t2 was created on orig_instance but its metadata is visible after the first rollback.
        //   Expected: data1 + data2 (rows 1, 2, 3, 4) are visible.
        //   BUG: 0 rows returned and SHOW DATA shows 0.
        def snapshot_content_to_t2 = """
        {
            "from_snapshot_id": "${snapshot_id_t2}",
            "from_instance_id": "rollback1_instance_id",
            "instance_id": "rollback2_instance_id",
            "name": "rollback2_instance",
            "is_successor": true
        }
        """
        logger.info("Second rollback to snapshot_t2: " + snapshot_content_to_t2)
        cluster.rollback(snapshot_content_to_t2)

        connectWithDockerCluster(cluster) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data after second rollback to t2 (4 rows expected): " + res.toString())
            assertEquals(res.size(), 4)
            assertEquals(res[0]['id'], 1)
            assertEquals(res[0]['name'], 'data1_a')
            assertEquals(res[1]['id'], 2)
            assertEquals(res[1]['name'], 'data1_b')
            assertEquals(res[2]['id'], 3)
            assertEquals(res[2]['name'], 'data2_a')
            assertEquals(res[3]['id'], 4)
            assertEquals(res[3]['name'], 'data2_b')

            // SHOW DATA must reflect non-empty table: files belonging to data1 and data2 must still exist.
            def showData = sql_return_maparray "SHOW DATA"
            logger.info("SHOW DATA after second rollback: " + showData.toString())
            def tableRow = showData.find { it['TableName'] == 'test_table' }
            assertNotNull(tableRow, "test_table must appear in SHOW DATA after second rollback")
            assertNotEquals(tableRow['Size'], '0.000 B',
                "SHOW DATA size must not be 0 for test_table after rollback to snapshot_t2 (data1 + data2 must be present)")
        }

        // rollback2_instance_id was created via rollback and references snapshot_t2 data (owned by orig_instance,
        // inherited through rollback1). Compact it, wait for completion, then decouple it before dropping rollback1.
        compact_snapshot(msHttpPort, "rollback2_instance_id")
        wait_compact_done(msHttpPort, "rollback2_instance_id")
        decouple_instance_api(msHttpPort, "rollback2_instance_id")

        // Final verification: after dropping the intermediate instance, rollback2 data must still be intact.
        connectWithDockerCluster(cluster) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Final data in rollback2_instance after dropping rollback1 (4 rows expected): " + res.toString())
            assertEquals(res.size(), 4)
            def showData = sql_return_maparray "SHOW DATA"
            logger.info("Final SHOW DATA after dropping rollback1_instance: " + showData.toString())
            def tableRow = showData.find { it['TableName'] == 'test_table' }
            assertNotNull(tableRow)
            assertNotEquals(tableRow['Size'], '0.000 B',
                "SHOW DATA size must not be 0 in the final state (snapshot_t2 data must be protected)")
        }
    }
}
