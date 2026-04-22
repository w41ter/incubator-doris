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

suite("test_clone_rollback_instance_invalid", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_clone_rollback_instance_invalid because not in cloud mode")
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

    def get_instance_api = { msHttpPort, instance_id, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/get_instance?token=greedisgood9999&instance_id=${instance_id}"
            check check_func
        }
    }

    def cluster_prefix = "regression_test_rollback_invalid_"
    def original_cluster = cluster_prefix + "original"

    def original_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "instance_rollback_invalid",
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

    def manual_init_clusters = [].toSet()
    dockers(["${original_cluster}": original_opt], manual_init_clusters) { clusters ->

        // Step 1: Create initial data and first snapshot
        String snapshot_id = ""
        String instance_id = "instance_rollback_invalid"

        connectWithDockerCluster(clusters[original_cluster]) {
            sql "CREATE DATABASE IF NOT EXISTS test_db_rollback_invalid"
            sql "USE test_db_rollback_invalid"

            sql """
                CREATE TABLE IF NOT EXISTS test_data (
                    id INT,
                    name VARCHAR(100),
                    value INT
                ) UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """

            sql "INSERT INTO test_data VALUES (1, 'Original1', 100)"
            sql "INSERT INTO test_data VALUES (2, 'Original2', 200)"
            sql "INSERT INTO test_data VALUES (3, 'Original3', 300)"

            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_before_corruption')"
            wait_snapshot_completed(clusters[original_cluster], "snapshot_before_corruption")
            snapshot_id = get_snapshot_id("snapshot_before_corruption")

            logger.info("Clean state snapshot created with ID: ${snapshot_id}")
        }

        // Step 2: Simulate bad operations (data corruption)
        connectWithDockerCluster(clusters[original_cluster]) {
            sql "USE test_db_rollback_invalid"

            logger.info("Simulating data corruption...")

            // Bad operation 1: Delete important data
            sql "DELETE FROM test_data WHERE id = 1"

            // Bad operation 2: Insert wrong data
            sql "INSERT INTO test_data VALUES (100, 'BadData1', -999)"
            sql "INSERT INTO test_data VALUES (101, 'BadData2', -888)"

            // Bad operation 3: Update with wrong values
            sql "UPDATE test_data SET value = -777 WHERE id = 2"

            // Verify corrupted state
            def corrupted_data = sql_return_maparray "SELECT * FROM test_data ORDER BY id"
            logger.info("Corrupted data state: " + corrupted_data.toString())
            assertEquals(corrupted_data.size(), 4)  // id=2,3,100,101 (id=1 was deleted)

            // Verify bad data exists
            def bad_data = corrupted_data.find { it['id'] == 100 }
            assertNotNull(bad_data)
            assertEquals(bad_data['value'], -999)
        }

        // Step 3: Perform rollback using cluster.rollback() method
        logger.info("Step 3: Performing rollback from snapshot...")

        // Prepare rollback snapshot configuration
        String new_instance_id = "${instance_id}_rollback_new"
        def cluster_snapshot_content = """
        {
            "name": "${new_instance_id}",
            "from_instance_id": "${instance_id}",
            "from_snapshot_id": "${snapshot_id}",
            "instance_id": "${new_instance_id}",
            "is_successor": true
        }
        """
        logger.info("Rollback snapshot configuration: " + cluster_snapshot_content)

        // Use the dedicated rollback method
        // This will: stop FE/BE, clean metadata/data, restart with snapshot
        clusters[original_cluster].rollback(cluster_snapshot_content)

        // Step 4: Verify data has been restored to snapshot state
        logger.info("Step 4: Verifying data restoration after rollback...")

        connectWithDockerCluster(clusters[original_cluster]) {
            sql "USE test_db_rollback_invalid"

            def restored_data = sql_return_maparray "SELECT * FROM test_data ORDER BY id"
            logger.info("Data after rollback: " + restored_data.toString())

            // After rollback, data should be restored to snapshot state
            assertEquals(restored_data.size(), 3, "Should have 3 original records")

            // Verify original data is restored
            assertEquals(restored_data[0]['id'], 1, "Record id=1 should be restored")
            assertEquals(restored_data[0]['name'], 'Original1')
            assertEquals(restored_data[0]['value'], 100)

            assertEquals(restored_data[1]['id'], 2)
            assertEquals(restored_data[1]['value'], 200, "Record id=2 should have original value")

            assertEquals(restored_data[2]['id'], 3)
            assertEquals(restored_data[2]['value'], 300)

            // Bad data should not exist
            def bad_data_100 = restored_data.find { it['id'] == 100 }
            assertNull(bad_data_100, "Corrupted data (id=100) should not exist after rollback")

            def bad_data_101 = restored_data.find { it['id'] == 101 }
            assertNull(bad_data_101, "Corrupted data (id=101) should not exist after rollback")

            logger.info("✓ Data successfully restored to snapshot state")
        }

        // Step 5: Verify instance is fully functional after rollback
        logger.info("Step 5: Verifying instance is fully functional after rollback...")

        connectWithDockerCluster(clusters[original_cluster]) {
            sql "USE test_db_rollback_invalid"

            // Should be able to write after rollback
            sql "INSERT INTO test_data VALUES (10, 'AfterRollback', 1000)"

            def data_after_write = sql_return_maparray "SELECT * FROM test_data WHERE id = 10"
            assertEquals(data_after_write.size(), 1, "Should be able to write after rollback")
            assertEquals(data_after_write[0]['id'], 10)
            assertEquals(data_after_write[0]['name'], 'AfterRollback')
            assertEquals(data_after_write[0]['value'], 1000)

            logger.info("✓ Instance is fully functional after rollback")
        }

        // Step 6: Verify instance metadata after rollback
        logger.info("Step 6: Verifying instance metadata after rollback...")

        def ms = clusters[original_cluster].getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort

        // Query using the ORIGINAL instance_id (external view)
        // MS should map it to the new instance internally
        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                logger.info("Instance metadata (queried with original instance_id): code=${respCode}, body=${body}")

                if (respCode == 200) {
                    def json = parseJson(body)
                    assertTrue(json.code?.equalsIgnoreCase("OK"), "get_instance should succeed")

                    def instance = json.result
                    assertNotNull(instance, "Instance metadata should exist")

                    logger.info("Instance ID: ${instance.instance_id}")
                    logger.info("Instance status: ${instance.status}")

                    // After rollback, querying with old instance_id still returns old instance metadata
                    // But it should have successor_instance_id pointing to the new instance
                    assertEquals(instance.instance_id, instance_id, "Should return the old instance metadata")
                    assertEquals(instance.status, "DELETED", "Instance should be DELETED after rollback")
                    assertEquals(instance.successor_instance_id, new_instance_id, "Should have successor_instance_id pointing to new instance")

                    logger.info("✓ Rollback successful: old instance has successor_instance_id = ${new_instance_id}")
                }
        }

        logger.info("Rollback instance invalidation test completed!")
    }
}
