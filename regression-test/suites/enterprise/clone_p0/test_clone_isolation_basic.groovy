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

suite("test_clone_isolation_basic", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_clone_isolation_basic because not in cloud mode")
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

    def cluster_prefix = "regression_test_clone_isolation_basic_"
    def base_name = cluster_prefix + "base"
    def derived_name = cluster_prefix + "derived"

    def base_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "base_instance_isolation_basic",
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
        instanceId: "derived_instance_isolation_basic",
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
        // Step 1: Create snapshot in base cluster with initial data
        String snapshot_id = ""
        connectWithDockerCluster(clusters[base_name]) {
            sql "CREATE DATABASE IF NOT EXISTS test_db_isolation"
            sql "USE test_db_isolation"

            // Create initial table in base cluster
            sql """
                CREATE TABLE IF NOT EXISTS base_table (
                    id INT,
                    name VARCHAR(100),
                    age INT
                ) UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO base_table VALUES (1, 'BaseUser1', 25)"
            sql "INSERT INTO base_table VALUES (2, 'BaseUser2', 30)"

            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_isolation_basic')"
            wait_snapshot_completed(clusters[base_name], "snapshot_isolation_basic")
            snapshot_id = get_snapshot_id("snapshot_isolation_basic")

            logger.info("Base cluster snapshot created with ID: ${snapshot_id}")

            // Write data AFTER snapshot in base cluster
            sql "INSERT INTO base_table VALUES (3, 'BaseUser3', 35)"
        }

        // Step 2: Restore the snapshot in derived cluster
        def cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id}",
            "from_instance_id": "base_instance_isolation_basic",
            "instance_id": "derived_instance_isolation_basic",
            "name": "derived_instance_isolation",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_clone_isolation_basic",
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

        // Step 3: Test isolation - operations in derived cluster should NOT affect base cluster
        connectWithDockerCluster(clusters[derived_name]) {
            sql "USE test_db_isolation"

            // Test 1: Verify initial snapshot data
            logger.info("Test 1: Verifying snapshot data in derived cluster...")
            def initial_data = sql_return_maparray "SELECT * FROM base_table ORDER BY id"
            assertEquals(initial_data.size(), 2)  // Only data before snapshot
            assertEquals(initial_data[0]['name'], 'BaseUser1')
            assertEquals(initial_data[1]['name'], 'BaseUser2')

            // Test 2: Create new table in derived cluster
            logger.info("Test 2: Creating new table in derived cluster...")
            sql """
                CREATE TABLE derived_only_table (
                    id INT,
                    info VARCHAR(100)
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO derived_only_table VALUES (1, 'DerivedData1')"
            sql "INSERT INTO derived_only_table VALUES (2, 'DerivedData2')"

            // Test 3: Insert data to existing table in derived cluster
            logger.info("Test 3: Inserting data to existing table in derived cluster...")
            sql "INSERT INTO base_table VALUES (100, 'DerivedUser100', 40)"
            sql "INSERT INTO base_table VALUES (101, 'DerivedUser101', 45)"

            // Test 4: Update data in derived cluster
            logger.info("Test 4: Updating data in derived cluster...")
            sql "UPDATE base_table SET age = 99 WHERE id = 1"

            // Test 5: Delete data in derived cluster
            logger.info("Test 5: Deleting data in derived cluster...")
            sql "DELETE FROM base_table WHERE id = 2"

            // Verify derived cluster state
            def derived_data = sql_return_maparray "SELECT * FROM base_table ORDER BY id"
            logger.info("Derived cluster data after operations: " + derived_data.toString())
            assertEquals(derived_data.size(), 3)  // 1 (updated), 100, 101 (2 was deleted)
            assertEquals(derived_data[0]['id'], 1)
            assertEquals(derived_data[0]['age'], 99)  // Updated
            assertEquals(derived_data[1]['id'], 100)
            assertEquals(derived_data[2]['id'], 101)
        }

        // Step 4: Verify base cluster is completely isolated and unchanged
        connectWithDockerCluster(clusters[base_name]) {
            sql "USE test_db_isolation"

            logger.info("Verifying base cluster isolation...")

            // Test 1: Verify new table does NOT exist in base cluster
            logger.info("Test 1: Verifying derived_only_table does NOT exist in base cluster...")
            test {
                sql "SELECT * FROM derived_only_table"
                exception "Table [derived_only_table] does not exist in database [test_db_isolation]"
            }

            // Test 2: Verify base_table data is unchanged
            logger.info("Test 2: Verifying base_table data is unchanged in base cluster...")
            def base_data = sql_return_maparray "SELECT * FROM base_table ORDER BY id"
            logger.info("Base cluster data: " + base_data.toString())

            // Should have original 2 records + 1 inserted after snapshot = 3 total
            assertEquals(base_data.size(), 3)

            // Original data unchanged
            assertEquals(base_data[0]['id'], 1)
            assertEquals(base_data[0]['name'], 'BaseUser1')
            assertEquals(base_data[0]['age'], 25)  // NOT 99 (not affected by derived update)

            assertEquals(base_data[1]['id'], 2)
            assertEquals(base_data[1]['name'], 'BaseUser2')  // NOT deleted (still exists)

            // Data inserted after snapshot
            assertEquals(base_data[2]['id'], 3)
            assertEquals(base_data[2]['name'], 'BaseUser3')

            // Test 3: Verify no data from derived cluster (100, 101) exists
            logger.info("Test 3: Verifying derived cluster data does NOT exist in base cluster...")
            def check_100 = sql_return_maparray "SELECT * FROM base_table WHERE id = 100"
            assertEquals(check_100.size(), 0)

            def check_101 = sql_return_maparray "SELECT * FROM base_table WHERE id = 101"
            assertEquals(check_101.size(), 0)
        }

        logger.info("Data isolation test completed successfully!")
    }
}
