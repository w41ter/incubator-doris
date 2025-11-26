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

suite("test_clone_table_models", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_clone_table_models because not in cloud mode")
        return
    }

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

    def get_snapshot_id = { snapshot_label ->
        def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots WHERE LABEL='${snapshot_label}'"
        assertEquals(res.size(), 1)
        return res[0]['ID']
    }

    def cluster_prefix = "regression_test_clone_table_models_"
    def base_name = cluster_prefix + "base"
    def derived_name = cluster_prefix + "derived"

    def base_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "base_instance_table_models",
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
        ],
        beConfigs: [
            "delete_bitmap_store_write_version=2",
            "delete_bitmap_store_read_version=2",
            "delete_bitmap_store_v2_max_bytes_in_fdb=0"
        ])

    def derive_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 0,
        instanceId: "derived_instance_table_models",
        externalMsCluster: base_name,
        msConfigs: [
            "enable_split_rowset_meta=true",
            "enable_split_tablet_schema_pb=true",
            "enable_multi_version_status=true",
            "enable_snapshot_data_migrator=true",
        ],
        beConfigs: [
            "delete_bitmap_store_write_version=2",
            "delete_bitmap_store_read_version=2",
            "delete_bitmap_store_v2_max_bytes_in_fdb=0"
        ])

    def manual_init_clusters = [derived_name].toSet()
    dockers(["${base_name}": base_opt, "${derived_name}": derive_opt], manual_init_clusters) { clusters ->
        // Step 1: Create tables with different models in the base cluster
        String snapshot_id = ""
        connectWithDockerCluster(clusters[base_name]) {
            sql "CREATE DATABASE IF NOT EXISTS test_db_models"
            sql "USE test_db_models"

            // Test 1: UNIQUE KEY table
            logger.info("Creating UNIQUE KEY table...")
            sql """
                CREATE TABLE IF NOT EXISTS test_unique (
                    id INT,
                    name VARCHAR(100),
                    age INT
                ) UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_unique VALUES (1, 'Alice', 25)"
            sql "INSERT INTO test_unique VALUES (2, 'Bob', 30)"
            sql "INSERT INTO test_unique VALUES (1, 'Alice Updated', 26)"  // Update via insert

            // Test 2: DUPLICATE KEY table
            logger.info("Creating DUPLICATE KEY table...")
            sql """
                CREATE TABLE IF NOT EXISTS test_duplicate (
                    id INT,
                    name VARCHAR(100),
                    timestamp DATETIME
                ) DUPLICATE KEY(id, name)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_duplicate VALUES (1, 'Event1', '2024-01-01 10:00:00')"
            sql "INSERT INTO test_duplicate VALUES (1, 'Event1', '2024-01-01 11:00:00')"
            sql "INSERT INTO test_duplicate VALUES (2, 'Event2', '2024-01-01 12:00:00')"

            // Test 3: AGGREGATE KEY table
            logger.info("Creating AGGREGATE KEY table...")
            sql """
                CREATE TABLE IF NOT EXISTS test_aggregate (
                    city VARCHAR(100),
                    date DATE,
                    pv BIGINT SUM,
                    uv BIGINT SUM
                ) AGGREGATE KEY(city, date)
                DISTRIBUTED BY HASH(city) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_aggregate VALUES ('Beijing', '2024-01-01', 100, 50)"
            sql "INSERT INTO test_aggregate VALUES ('Beijing', '2024-01-01', 200, 80)"
            sql "INSERT INTO test_aggregate VALUES ('Shanghai', '2024-01-01', 150, 60)"

            // Test 4: MOW (Merge-On-Write) UNIQUE KEY table
            logger.info("Creating MOW UNIQUE KEY table...")
            sql """
                CREATE TABLE IF NOT EXISTS test_mow (
                    id INT,
                    name VARCHAR(100),
                    value INT,
                    update_time DATETIME
                ) UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true"
                )
            """
            sql "INSERT INTO test_mow VALUES (1, 'MOW_1', 100, '2024-01-01 10:00:00')"
            sql "INSERT INTO test_mow VALUES (2, 'MOW_2', 200, '2024-01-01 10:00:00')"
            sql "INSERT INTO test_mow VALUES (3, 'MOW_3', 300, '2024-01-01 10:00:00')"
            // Update via insert (MOW will handle merge efficiently)
            sql "INSERT INTO test_mow VALUES (1, 'MOW_1_Updated', 150, '2024-01-01 11:00:00')"
            sql "INSERT INTO test_mow VALUES (2, 'MOW_2_Updated', 250, '2024-01-01 11:00:00')"

            // Create snapshot
            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_table_models')"
            wait_snapshot_completed(clusters[base_name], "snapshot_table_models")
            snapshot_id = get_snapshot_id("snapshot_table_models")

            logger.info("Base cluster snapshot created with ID: ${snapshot_id}")

            // Insert more data after snapshot (should not appear in derived cluster)
            sql "INSERT INTO test_unique VALUES (3, 'Charlie', 35)"
            sql "INSERT INTO test_duplicate VALUES (3, 'Event3', '2024-01-01 13:00:00')"
            sql "INSERT INTO test_aggregate VALUES ('Shenzhen', '2024-01-01', 180, 70)"
            sql "INSERT INTO test_mow VALUES (4, 'MOW_4', 400, '2024-01-01 12:00:00')"
        }

        // Step 2: Restore the snapshot in the derived cluster
        def cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id}",
            "from_instance_id": "base_instance_table_models",
            "instance_id": "derived_instance_table_models",
            "name": "derived_instance_models",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_clone_table_models",
                "endpoint": "${getS3Endpoint()}",
                "external_endpoint": "${getS3Endpoint()}",
                "region": "${getS3Region()}",
                "provider": "${getS3Provider()}"
            }
        }
        """
        logger.info("Setup derived cluster with snapshot: " + cluster_snapshot_content)
        derive_opt.clusterSnapshot = cluster_snapshot_content
        clusters[derived_name].init(derive_opt, true)

        // Step 3: Verify data in derived cluster
        connectWithDockerCluster(clusters[derived_name]) {
            sql "USE test_db_models"

            // Verify UNIQUE KEY table
            logger.info("Verifying UNIQUE KEY table in derived cluster...")
            def unique_res = sql_return_maparray "SELECT * FROM test_unique ORDER BY id"
            logger.info("UNIQUE table data: " + unique_res.toString())
            assertEquals(unique_res.size(), 2)
            assertEquals(unique_res[0]['id'], 1)
            assertEquals(unique_res[0]['name'], 'Alice Updated')
            assertEquals(unique_res[0]['age'], 26)
            assertEquals(unique_res[1]['id'], 2)
            assertEquals(unique_res[1]['name'], 'Bob')

            // Verify DUPLICATE KEY table
            logger.info("Verifying DUPLICATE KEY table in derived cluster...")
            def dup_res = sql_return_maparray "SELECT * FROM test_duplicate ORDER BY id, timestamp"
            logger.info("DUPLICATE table data: " + dup_res.toString())
            assertEquals(dup_res.size(), 3)
            assertEquals(dup_res[0]['id'], 1)
            assertEquals(dup_res[1]['id'], 1)
            assertEquals(dup_res[2]['id'], 2)

            // Verify AGGREGATE KEY table
            logger.info("Verifying AGGREGATE KEY table in derived cluster...")
            def agg_res = sql_return_maparray "SELECT * FROM test_aggregate ORDER BY city"
            logger.info("AGGREGATE table data: " + agg_res.toString())
            assertEquals(agg_res.size(), 2)
            assertEquals(agg_res[0]['city'], 'Beijing')
            assertEquals(agg_res[0]['pv'], 300)  // 100 + 200 aggregated
            assertEquals(agg_res[0]['uv'], 130)  // 50 + 80 aggregated
            assertEquals(agg_res[1]['city'], 'Shanghai')
            assertEquals(agg_res[1]['pv'], 150)

            // Verify MOW table
            logger.info("Verifying MOW (Merge-On-Write) table in derived cluster...")
            def mow_res = sql_return_maparray "SELECT * FROM test_mow ORDER BY id"
            logger.info("MOW table data: " + mow_res.toString())
            assertEquals(mow_res.size(), 3)  // id=1,2,3 (updates merged)
            assertEquals(mow_res[0]['id'], 1)
            assertEquals(mow_res[0]['name'], 'MOW_1_Updated')
            assertEquals(mow_res[0]['value'], 150)  // Updated value
            assertEquals(mow_res[1]['id'], 2)
            assertEquals(mow_res[1]['name'], 'MOW_2_Updated')
            assertEquals(mow_res[1]['value'], 250)  // Updated value
            assertEquals(mow_res[2]['id'], 3)
            assertEquals(mow_res[2]['value'], 300)  // Original value

            // Verify no post-snapshot data in MOW table
            def check_mow_4 = mow_res.find { it['id'] == 4 }
            assertNull(check_mow_4, "Post-snapshot MOW data should not exist")

            // Test write operations on derived cluster (including MOW)
            logger.info("Testing write operations on derived cluster...")
            sql "INSERT INTO test_unique VALUES (4, 'David', 40)"
            sql "INSERT INTO test_duplicate VALUES (4, 'Event4', '2024-01-01 14:00:00')"
            sql "INSERT INTO test_aggregate VALUES ('Guangzhou', '2024-01-01', 120, 55)"

            // Test MOW update
            sql "INSERT INTO test_mow VALUES (100, 'Derived_MOW', 1000, '2024-01-01 13:00:00')"
            sql "INSERT INTO test_mow VALUES (1, 'MOW_1_Derived_Update', 999, '2024-01-01 14:00:00')"

            def unique_after = sql_return_maparray "SELECT * FROM test_unique ORDER BY id"
            assertEquals(unique_after.size(), 3)
            assertEquals(unique_after[2]['name'], 'David')

            def mow_after = sql_return_maparray "SELECT * FROM test_mow ORDER BY id"
            assertEquals(mow_after.size(), 4)  // 1,2,3,100
            assertEquals(mow_after[0]['name'], 'MOW_1_Derived_Update')  // Updated again
            assertEquals(mow_after[0]['value'], 999)
            assertEquals(mow_after[3]['id'], 100)
        }

        // Step 4: Verify base cluster data remains unchanged
        connectWithDockerCluster(clusters[base_name]) {
            sql "USE test_db_models"

            def unique_base = sql_return_maparray "SELECT * FROM test_unique ORDER BY id"
            logger.info("Base cluster UNIQUE table data: " + unique_base.toString())
            assertEquals(unique_base.size(), 3)
            assertEquals(unique_base[2]['name'], 'Charlie')  // Data inserted after snapshot

            def agg_base = sql_return_maparray "SELECT * FROM test_aggregate ORDER BY city"
            assertEquals(agg_base.size(), 3)  // Including Shenzhen

            def mow_base = sql_return_maparray "SELECT * FROM test_mow ORDER BY id"
            logger.info("Base cluster MOW table data: " + mow_base.toString())
            assertEquals(mow_base.size(), 4)  // id=1,2,3,4
            assertEquals(mow_base[0]['name'], 'MOW_1_Updated')  // NOT 'MOW_1_Derived_Update'
            assertEquals(mow_base[0]['value'], 150)  // NOT 999
            assertEquals(mow_base[3]['id'], 4)  // Post-snapshot data exists
            assertEquals(mow_base[3]['value'], 400)
        }

        logger.info("All table model tests completed successfully!")
    }
}
