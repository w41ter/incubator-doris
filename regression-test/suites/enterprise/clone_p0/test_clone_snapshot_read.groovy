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

suite("test_clone_snapshot_read", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_clone_snapshot_read because not in cloud mode")
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

    def cluster_prefix = "regression_test_clone_snapshot_read_"
    def base_name = cluster_prefix + "base"
    def derived_name = cluster_prefix + "derived"

    def base_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "base_instance_snapshot_read",
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
        instanceId: "derived_instance_snapshot_read",
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
        // Step 1: Create tables with various data in base cluster, then snapshot
        String snapshot_id = ""
        connectWithDockerCluster(clusters[base_name]) {
            sql "CREATE DATABASE IF NOT EXISTS test_db_snapshot_read"
            sql "USE test_db_snapshot_read"

            // Table 1: UNIQUE KEY table with snapshot data
            sql """
                CREATE TABLE IF NOT EXISTS test_unique_snapshot (
                    id INT,
                    name VARCHAR(100),
                    age INT,
                    city VARCHAR(100)
                ) UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_unique_snapshot VALUES (1, 'Alice', 25, 'Beijing')"
            sql "INSERT INTO test_unique_snapshot VALUES (2, 'Bob', 30, 'Shanghai')"
            sql "INSERT INTO test_unique_snapshot VALUES (3, 'Charlie', 35, 'Shenzhen')"
            sql "INSERT INTO test_unique_snapshot VALUES (4, 'David', 40, 'Guangzhou')"
            sql "INSERT INTO test_unique_snapshot VALUES (5, 'Eva', 28, 'Hangzhou')"

            // Table 2: DUPLICATE KEY table with snapshot data
            sql """
                CREATE TABLE IF NOT EXISTS test_duplicate_snapshot (
                    id INT,
                    event VARCHAR(100),
                    timestamp DATETIME
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_duplicate_snapshot VALUES (1, 'Login', '2024-01-01 10:00:00')"
            sql "INSERT INTO test_duplicate_snapshot VALUES (1, 'Login', '2024-01-01 11:00:00')"
            sql "INSERT INTO test_duplicate_snapshot VALUES (2, 'Logout', '2024-01-01 12:00:00')"
            sql "INSERT INTO test_duplicate_snapshot VALUES (3, 'Purchase', '2024-01-01 13:00:00')"

            // Table 3: AGGREGATE KEY table with snapshot data
            sql """
                CREATE TABLE IF NOT EXISTS test_aggregate_snapshot (
                    product_id INT,
                    date DATE,
                    sales BIGINT SUM,
                    quantity BIGINT SUM
                ) AGGREGATE KEY(product_id, date)
                DISTRIBUTED BY HASH(product_id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_aggregate_snapshot VALUES (100, '2024-01-01', 1000, 10)"
            sql "INSERT INTO test_aggregate_snapshot VALUES (100, '2024-01-01', 2000, 20)"
            sql "INSERT INTO test_aggregate_snapshot VALUES (101, '2024-01-01', 1500, 15)"

            // Table 4: Partitioned table with snapshot data
            sql """
                CREATE TABLE IF NOT EXISTS test_partition_snapshot (
                    id INT,
                    date DATE,
                    name VARCHAR(100)
                ) UNIQUE KEY(id, date)
                PARTITION BY RANGE(date) (
                    PARTITION p202401 VALUES LESS THAN ('2024-02-01'),
                    PARTITION p202402 VALUES LESS THAN ('2024-03-01')
                )
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_partition_snapshot VALUES (1, '2024-01-15', 'Jan Data 1')"
            sql "INSERT INTO test_partition_snapshot VALUES (2, '2024-01-20', 'Jan Data 2')"
            sql "INSERT INTO test_partition_snapshot VALUES (3, '2024-02-15', 'Feb Data 1')"

            // Create snapshot
            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_read_test')"
            wait_snapshot_completed(clusters[base_name], "snapshot_read_test")
            snapshot_id = get_snapshot_id("snapshot_read_test")

            logger.info("Base cluster snapshot created with ID: ${snapshot_id}")

            // Insert data AFTER snapshot in base cluster (should NOT appear in derived cluster)
            sql "INSERT INTO test_unique_snapshot VALUES (100, 'PostSnapshot', 50, 'Chengdu')"
            sql "INSERT INTO test_duplicate_snapshot VALUES (100, 'PostSnapshotEvent', '2024-01-01 20:00:00')"
            sql "INSERT INTO test_aggregate_snapshot VALUES (200, '2024-01-02', 5000, 50)"
            sql "INSERT INTO test_partition_snapshot VALUES (100, '2024-01-25', 'PostSnapshot')"
        }

        // Step 2: Restore snapshot in derived cluster
        def cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id}",
            "from_instance_id": "base_instance_snapshot_read",
            "instance_id": "derived_instance_snapshot_read",
            "name": "derived_instance_snapshot_read",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_clone_snapshot_read",
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

        // Step 3: Test snapshot read in derived cluster
        connectWithDockerCluster(clusters[derived_name]) {
            sql "USE test_db_snapshot_read"

            // Test 1: Read all snapshot data from UNIQUE KEY table (no modifications)
            logger.info("Test 1: Reading unmodified snapshot data from UNIQUE KEY table...")
            def unique_data = sql_return_maparray "SELECT * FROM test_unique_snapshot ORDER BY id"
            logger.info("UNIQUE table snapshot data: " + unique_data.toString())
            assertEquals(unique_data.size(), 5)  // Only snapshot data
            assertEquals(unique_data[0]['id'], 1)
            assertEquals(unique_data[0]['name'], 'Alice')
            assertEquals(unique_data[0]['age'], 25)
            assertEquals(unique_data[0]['city'], 'Beijing')
            assertEquals(unique_data[4]['id'], 5)
            assertEquals(unique_data[4]['name'], 'Eva')

            // Verify no post-snapshot data
            def check_post_snapshot = sql_return_maparray "SELECT * FROM test_unique_snapshot WHERE id = 100"
            assertEquals(check_post_snapshot.size(), 0)

            // Test 2: Read snapshot data and then modify some records
            logger.info("Test 2: Reading snapshot data, then modifying specific records...")

            // First read specific unmodified records
            def alice_before = sql_return_maparray "SELECT * FROM test_unique_snapshot WHERE id = 1"
            assertEquals(alice_before.size(), 1)
            assertEquals(alice_before[0]['name'], 'Alice')
            assertEquals(alice_before[0]['age'], 25)

            def charlie_before = sql_return_maparray "SELECT * FROM test_unique_snapshot WHERE id = 3"
            assertEquals(charlie_before.size(), 1)
            assertEquals(charlie_before[0]['name'], 'Charlie')
            assertEquals(charlie_before[0]['age'], 35)

            // Modify one record (id=1)
            sql "UPDATE test_unique_snapshot SET age = 26 WHERE id = 1"

            // Read modified record
            def alice_after = sql_return_maparray "SELECT * FROM test_unique_snapshot WHERE id = 1"
            assertEquals(alice_after.size(), 1)
            assertEquals(alice_after[0]['age'], 26)  // Modified

            // Read unmodified snapshot records (should still be accessible)
            def charlie_after = sql_return_maparray "SELECT * FROM test_unique_snapshot WHERE id = 3"
            assertEquals(charlie_after.size(), 1)
            assertEquals(charlie_after[0]['name'], 'Charlie')
            assertEquals(charlie_after[0]['age'], 35)  // Unchanged

            def eva_after = sql_return_maparray "SELECT * FROM test_unique_snapshot WHERE id = 5"
            assertEquals(eva_after.size(), 1)
            assertEquals(eva_after[0]['name'], 'Eva')
            assertEquals(eva_after[0]['age'], 28)  // Unchanged

            // Test 3: Read snapshot data from DUPLICATE KEY table
            logger.info("Test 3: Reading snapshot data from DUPLICATE KEY table...")
            def dup_data = sql_return_maparray "SELECT * FROM test_duplicate_snapshot ORDER BY id, timestamp"
            logger.info("DUPLICATE table snapshot data: " + dup_data.toString())
            assertEquals(dup_data.size(), 4)  // Only snapshot data
            assertEquals(dup_data[0]['id'], 1)
            assertEquals(dup_data[0]['event'], 'Login')
            assertEquals(dup_data[3]['id'], 3)
            assertEquals(dup_data[3]['event'], 'Purchase')

            // Insert new data
            sql "INSERT INTO test_duplicate_snapshot VALUES (4, 'NewEvent', '2024-01-01 15:00:00')"

            // Read unmodified snapshot data (should coexist with new data)
            def mixed_data = sql_return_maparray "SELECT * FROM test_duplicate_snapshot WHERE id <= 3 ORDER BY id, timestamp"
            assertEquals(mixed_data.size(), 4)  // Original snapshot data intact

            // Test 4: Read snapshot data from AGGREGATE KEY table
            logger.info("Test 4: Reading snapshot data from AGGREGATE KEY table...")
            def agg_data = sql_return_maparray "SELECT * FROM test_aggregate_snapshot ORDER BY product_id, date"
            logger.info("AGGREGATE table snapshot data: " + agg_data.toString())
            assertEquals(agg_data.size(), 2)
            assertEquals(agg_data[0]['product_id'], 100)
            assertEquals(agg_data[0]['sales'], 3000)  // 1000 + 2000 aggregated
            assertEquals(agg_data[0]['quantity'], 30)  // 10 + 20 aggregated
            assertEquals(agg_data[1]['product_id'], 101)
            assertEquals(agg_data[1]['sales'], 1500)

            // Insert new aggregate data for different product
            sql "INSERT INTO test_aggregate_snapshot VALUES (102, '2024-01-01', 2500, 25)"

            // Read original snapshot aggregate data (should be unchanged)
            def agg_100 = sql_return_maparray "SELECT * FROM test_aggregate_snapshot WHERE product_id = 100"
            assertEquals(agg_100.size(), 1)
            assertEquals(agg_100[0]['sales'], 3000)  // Still aggregated correctly

            // Test 5: Read snapshot data from partitioned table
            logger.info("Test 5: Reading snapshot data from partitioned table...")
            def partition_data = sql_return_maparray "SELECT * FROM test_partition_snapshot ORDER BY id"
            logger.info("Partitioned table snapshot data: " + partition_data.toString())
            assertEquals(partition_data.size(), 3)
            assertEquals(partition_data[0]['id'], 1)
            assertEquals(partition_data[0]['name'], 'Jan Data 1')
            assertEquals(partition_data[2]['id'], 3)
            assertEquals(partition_data[2]['name'], 'Feb Data 1')

            // Insert new data into a partition
            sql "INSERT INTO test_partition_snapshot VALUES (4, '2024-01-25', 'New Jan Data')"

            // Read unmodified partition data
            def p202401_snapshot = sql_return_maparray "SELECT * FROM test_partition_snapshot WHERE id <= 2 ORDER BY id"
            assertEquals(p202401_snapshot.size(), 2)
            assertEquals(p202401_snapshot[0]['name'], 'Jan Data 1')
            assertEquals(p202401_snapshot[1]['name'], 'Jan Data 2')

            def p202402_snapshot = sql_return_maparray "SELECT * FROM test_partition_snapshot WHERE id = 3"
            assertEquals(p202402_snapshot.size(), 1)
            assertEquals(p202402_snapshot[0]['name'], 'Feb Data 1')

            // Test 6: Complex query on snapshot data
            logger.info("Test 6: Complex queries on snapshot data...")

            // Join between snapshot tables
            def join_result = sql_return_maparray """
                SELECT u.id, u.name, u.city
                FROM test_unique_snapshot u
                WHERE u.age > 30 AND u.id <= 5
                ORDER BY u.id
            """
            logger.info("Join result: " + join_result.toString())
            assertEquals(join_result.size(), 2)  // Charlie(35), David(40)
            assertEquals(join_result[0]['name'], 'Charlie')
            assertEquals(join_result[1]['name'], 'David')

            // Aggregate query on snapshot data
            def agg_query = sql_return_maparray """
                SELECT COUNT(*) as cnt, AVG(age) as avg_age
                FROM test_unique_snapshot
                WHERE id <= 5
            """
            logger.info("Aggregate query result: " + agg_query.toString())
            assertEquals(agg_query[0]['cnt'], 5)
            // Average age: (26 + 30 + 35 + 40 + 28) / 5 = 31.8 (id=1 was updated to 26)

            // Test 7: Delete some records, then verify undeleted snapshot data is still readable
            logger.info("Test 7: Delete records, verify undeleted snapshot data...")
            sql "DELETE FROM test_unique_snapshot WHERE id = 2"

            def after_delete = sql_return_maparray "SELECT * FROM test_unique_snapshot ORDER BY id"
            assertEquals(after_delete.size(), 4)  // 5 - 1 deleted

            // Verify undeleted snapshot records are still readable
            def david = after_delete.find { it['id'] == 4 }
            assertNotNull(david)
            assertEquals(david['name'], 'David')
            assertEquals(david['age'], 40)

            def eva = after_delete.find { it['id'] == 5 }
            assertNotNull(eva)
            assertEquals(eva['name'], 'Eva')
            assertEquals(eva['age'], 28)
        }

        // Step 4: Verify base cluster data is unaffected
        connectWithDockerCluster(clusters[base_name]) {
            sql "USE test_db_snapshot_read"

            logger.info("Verifying base cluster data is unaffected...")

            def base_unique = sql_return_maparray "SELECT * FROM test_unique_snapshot ORDER BY id"
            logger.info("Base cluster UNIQUE table: " + base_unique.toString())
            assertEquals(base_unique.size(), 6)  // 5 snapshot + 1 post-snapshot

            // Verify original snapshot data unchanged
            assertEquals(base_unique[0]['id'], 1)
            assertEquals(base_unique[0]['age'], 25)  // NOT 26 (not updated)

            // Verify id=2 still exists (not deleted)
            def bob = base_unique.find { it['id'] == 2 }
            assertNotNull(bob)
            assertEquals(bob['name'], 'Bob')

            // Verify post-snapshot data exists
            assertEquals(base_unique[5]['id'], 100)
            assertEquals(base_unique[5]['name'], 'PostSnapshot')
        }

        logger.info("Snapshot read test completed successfully!")
    }
}
