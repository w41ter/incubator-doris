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

// Regression test for the "double rollback data loss" bug:
//   1. On orig_instance: write data1, take snapshot_t1, write data2, take snapshot_t2, write data3 (no snapshot).
//   2. Rollback orig_instance → rollback1_instance (from snapshot_t1): only data1 visible. ✓
//   3. Rollback rollback1_instance → rollback2_instance (from snapshot_t2, from_instance_id=rollback1_instance_id):
//      EXPECT data1 + data2 visible.  BUG: 0 rows returned and SHOW DATA = 0.
suite("test_rollback_double_snapshot_simple", "snapshot,docker") {
    if (!isCloudMode()) {
        logger.info("Skip test_rollback_double_snapshot_simple because not in cloud mode")
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
        ],
        recycleConfigs: [
            "recycle_interval_seconds=1",
            "recycler_sleep_before_scheduling_seconds=1",
            "enable_snapshot_data_migrator=true",
        ])

    docker(opt) {
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
    }
}
