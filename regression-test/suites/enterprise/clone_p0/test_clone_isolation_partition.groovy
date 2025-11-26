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

suite("test_clone_isolation_partition", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_clone_isolation_partition because not in cloud mode")
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

    def wait_partition_operation_finish = { cluster, database, table_name ->
        // Partition operations (ADD/DROP/TRUNCATE) are usually synchronous in Doris
        // Just add a small sleep to ensure the operation is fully applied
        sleep(1000)
    }

    def cluster_prefix = "regression_test_clone_isolation_partition_"
    def base_name = cluster_prefix + "base"
    def derived_name = cluster_prefix + "derived"

    def base_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "base_instance_isolation_partition",
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
        instanceId: "derived_instance_isolation_partition",
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
        // Step 1: Create partitioned table and snapshot in base cluster
        String snapshot_id = ""
        connectWithDockerCluster(clusters[base_name]) {
            sql "CREATE DATABASE IF NOT EXISTS test_db_partition"
            sql "USE test_db_partition"

            // Create partitioned table
            sql """
                CREATE TABLE IF NOT EXISTS test_partition (
                    id INT,
                    date DATE,
                    name VARCHAR(100)
                ) UNIQUE KEY(id, date)
                PARTITION BY RANGE(date) (
                    PARTITION p202401 VALUES LESS THAN ('2024-02-01'),
                    PARTITION p202402 VALUES LESS THAN ('2024-03-01'),
                    PARTITION p202403 VALUES LESS THAN ('2024-04-01')
                )
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """

            // Insert data into different partitions
            sql "INSERT INTO test_partition VALUES (1, '2024-01-15', 'Jan1')"
            sql "INSERT INTO test_partition VALUES (2, '2024-01-20', 'Jan2')"
            sql "INSERT INTO test_partition VALUES (3, '2024-02-15', 'Feb1')"
            sql "INSERT INTO test_partition VALUES (4, '2024-02-20', 'Feb2')"
            sql "INSERT INTO test_partition VALUES (5, '2024-03-15', 'Mar1')"

            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_partition')"
            wait_snapshot_completed(clusters[base_name], "snapshot_partition")
            snapshot_id = get_snapshot_id("snapshot_partition")

            logger.info("Base cluster snapshot created with ID: ${snapshot_id}")

            // Add more data after snapshot
            sql "INSERT INTO test_partition VALUES (6, '2024-03-20', 'Mar2')"
        }

        // Step 2: Restore snapshot in derived cluster
        def cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id}",
            "from_instance_id": "base_instance_isolation_partition",
            "instance_id": "derived_instance_isolation_partition",
            "name": "derived_instance_partition",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_clone_isolation_partition",
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

        // Step 3: Perform partition operations in derived cluster
        connectWithDockerCluster(clusters[derived_name]) {
            sql "USE test_db_partition"

            // Verify initial data from snapshot
            logger.info("Verifying initial snapshot data in derived cluster...")
            def initial_data = sql_return_maparray "SELECT * FROM test_partition ORDER BY id"
            logger.info("Initial data: " + initial_data.toString())
            assertEquals(initial_data.size(), 5)  // Data before snapshot

            // Test 1: ALTER TABLE ADD PARTITION
            logger.info("Test 1: ADD PARTITION in derived cluster...")
            sql "ALTER TABLE test_partition ADD PARTITION p202404 VALUES LESS THAN ('2024-05-01')"
            wait_partition_operation_finish(clusters[derived_name], "test_db_partition", "test_partition")

            def partitions_after_add = sql_return_maparray "SHOW PARTITIONS FROM test_partition"
            logger.info("Partitions after ADD: " + partitions_after_add.toString())
            assertEquals(partitions_after_add.size(), 4)

            // Insert data into new partition
            sql "INSERT INTO test_partition VALUES (100, '2024-04-15', 'Apr1')"

            // Test 2: ALTER TABLE DROP PARTITION
            logger.info("Test 2: DROP PARTITION in derived cluster...")
            sql "ALTER TABLE test_partition DROP PARTITION p202401"
            wait_partition_operation_finish(clusters[derived_name], "test_db_partition", "test_partition")

            def partitions_after_drop = sql_return_maparray "SHOW PARTITIONS FROM test_partition"
            logger.info("Partitions after DROP: " + partitions_after_drop.toString())
            assertEquals(partitions_after_drop.size(), 3)  // p202402, p202403, p202404

            // Verify data in dropped partition is gone
            def data_after_drop = sql_return_maparray "SELECT * FROM test_partition WHERE date < '2024-02-01'"
            assertEquals(data_after_drop.size(), 0)

            // Test 3: Insert data into existing partition
            logger.info("Test 3: INSERT into existing partition in derived cluster...")
            sql "INSERT INTO test_partition VALUES (200, '2024-02-25', 'Feb3')"

            // Test 4: TRUNCATE PARTITION
            logger.info("Test 4: TRUNCATE PARTITION in derived cluster...")
            sql "TRUNCATE TABLE test_partition PARTITION p202402"
            wait_partition_operation_finish(clusters[derived_name], "test_db_partition", "test_partition")

            def data_after_truncate = sql_return_maparray "SELECT * FROM test_partition WHERE date >= '2024-02-01' AND date < '2024-03-01'"
            logger.info("Data in p202402 after TRUNCATE: " + data_after_truncate.toString())
            assertEquals(data_after_truncate.size(), 0)  // Partition data cleared

            // Verify other partitions are unaffected
            def remaining_data = sql_return_maparray "SELECT * FROM test_partition ORDER BY id"
            logger.info("Remaining data in derived cluster: " + remaining_data.toString())
            assertEquals(remaining_data.size(), 2)  // id=5 (Mar1), id=100 (Apr1)
        }

        // Step 4: Verify base cluster partition structure and data are unchanged
        connectWithDockerCluster(clusters[base_name]) {
            sql "USE test_db_partition"

            logger.info("Verifying base cluster partition isolation...")

            // Test 1: Original partitions should all exist
            logger.info("Test 1: Verifying original partitions exist in base cluster...")
            def base_partitions = sql_return_maparray "SHOW PARTITIONS FROM test_partition"
            logger.info("Base cluster partitions: " + base_partitions.toString())
            assertEquals(base_partitions.size(), 3)  // p202401, p202402, p202403 (original)

            def partition_names = base_partitions.collect { it['PartitionName'] }
            assertTrue(partition_names.contains('p202401'))  // NOT dropped
            assertTrue(partition_names.contains('p202402'))
            assertTrue(partition_names.contains('p202403'))
            assertFalse(partition_names.contains('p202404'))  // NOT added

            // Test 2: All original data should be intact
            logger.info("Test 2: Verifying all original data exists in base cluster...")
            def base_data = sql_return_maparray "SELECT * FROM test_partition ORDER BY id"
            logger.info("Base cluster data: " + base_data.toString())
            assertEquals(base_data.size(), 6)  // 5 original + 1 inserted after snapshot

            // Verify p202401 data (should NOT be dropped)
            def p202401_data = base_data.findAll { it['id'] == 1 || it['id'] == 2 }
            assertEquals(p202401_data.size(), 2)

            // Verify p202402 data (should NOT be truncated)
            def p202402_data = base_data.findAll { it['id'] == 3 || it['id'] == 4 }
            assertEquals(p202402_data.size(), 2)

            // Verify no derived cluster data exists
            def check_100 = base_data.findAll { it['id'] == 100 }
            assertEquals(check_100.size(), 0)

            def check_200 = base_data.findAll { it['id'] == 200 }
            assertEquals(check_200.size(), 0)
        }

        logger.info("Partition isolation test completed successfully!")
    }
}
