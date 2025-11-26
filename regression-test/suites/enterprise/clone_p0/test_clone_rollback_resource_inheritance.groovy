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
import groovy.json.JsonSlurper
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.suite.SuiteCluster
import org.awaitility.Awaitility

suite("test_clone_rollback_resource_inheritance", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_clone_rollback_resource_inheritance because not in cloud mode")
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

    def cluster_prefix = "regression_test_rollback_resource_"
    def base_cluster = cluster_prefix + "base"

    def base_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "instance_resource",
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
    dockers(["${base_cluster}": base_opt], manual_init_clusters) { clusters ->

        // Step 1: Create data and snapshot in instance
        String snapshot_id = ""
        String instance_id = "instance_resource"

        connectWithDockerCluster(clusters[base_cluster]) {
            sql "CREATE DATABASE IF NOT EXISTS test_db_resource"
            sql "USE test_db_resource"

            sql """
                CREATE TABLE IF NOT EXISTS resource_table (
                    id INT,
                    resource_name VARCHAR(100),
                    value INT
                ) UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """

            sql "INSERT INTO resource_table VALUES (1, 'CPU', 80)"
            sql "INSERT INTO resource_table VALUES (2, 'Memory', 16)"
            sql "INSERT INTO resource_table VALUES (3, 'Disk', 500)"

            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_resource')"
            wait_snapshot_completed(clusters[base_cluster], "snapshot_resource")
            snapshot_id = get_snapshot_id("snapshot_resource")

            logger.info("Snapshot created with ID: ${snapshot_id}")

            // Make some changes after snapshot (simulate mistakes)
            sql "DELETE FROM resource_table WHERE id = 1"
            sql "UPDATE resource_table SET value = -1 WHERE id = 2"
            sql "INSERT INTO resource_table VALUES (100, 'BadResource', -999)"
        }

        // Step 2: Perform rollback using cluster.rollback() method
        logger.info("Step 2: Performing rollback from snapshot...")

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
        clusters[base_cluster].rollback(cluster_snapshot_content)

        // Step 3: Verify data was restored from snapshot (rollback succeeded)
        connectWithDockerCluster(clusters[base_cluster]) {
            sql "USE test_db_resource"

            logger.info("Test 1: Verifying data inheritance from snapshot...")
            def rollback_data = sql_return_maparray "SELECT * FROM resource_table ORDER BY id"
            logger.info("Rollback instance data: " + rollback_data.toString())

            // Should have snapshot data, not corrupted data
            assertEquals(rollback_data.size(), 3)
            assertEquals(rollback_data[0]['id'], 1)
            assertEquals(rollback_data[0]['resource_name'], 'CPU')
            assertEquals(rollback_data[0]['value'], 80)  // Original value
            assertEquals(rollback_data[1]['id'], 2)
            assertEquals(rollback_data[1]['value'], 16)  // Original value, not -1
            assertEquals(rollback_data[2]['id'], 3)

            // Bad data should not exist
            def bad_data = rollback_data.find { it['id'] == 100 }
            assertNull(bad_data, "Bad data should not exist in rollback instance")

            logger.info("Test 2: Verifying database and table schema inheritance...")
            def databases = sql_return_maparray "SHOW DATABASES"
            logger.info("Databases: " + databases.toString())
            def test_db = databases.find { it['Database'] == 'test_db_resource' }
            assertNotNull(test_db, "Database should be inherited")

            def tables = sql_return_maparray "SHOW TABLES"
            logger.info("Tables: " + tables.toString())
            def resource_table = tables.find { it['Tables_in_test_db_resource'] == 'resource_table' }
            assertNotNull(resource_table, "Table should be inherited")

            def schema = sql_return_maparray "DESC resource_table"
            logger.info("Table schema: " + schema.toString())
            assertEquals(schema.size(), 3)  // id, resource_name, value

            logger.info("Test 3: Verifying instance is fully functional after rollback...")
            // Can insert new data
            sql "INSERT INTO resource_table VALUES (10, 'NewResource', 999)"

            // Can update data
            sql "UPDATE resource_table SET value = 85 WHERE id = 1"

            // Can delete data
            sql "DELETE FROM resource_table WHERE id = 3"

            def final_data = sql_return_maparray "SELECT * FROM resource_table ORDER BY id"
            logger.info("Data after operations: " + final_data.toString())
            assertEquals(final_data.size(), 3)  // id=1,2,10 (id=3 deleted)

            logger.info("✓ Instance is fully functional after rollback")
        }

        // Step 4: Verify instance metadata after rollback
        def ms = clusters[base_cluster].getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort

        logger.info("Test 4: Verifying instance metadata after rollback...")
        // Query using the ORIGINAL instance_id (external view)
        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                logger.info("Instance metadata (queried with original instance_id): ${body}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))

                def instance = json.result
                assertNotNull(instance, "Instance metadata should exist")

                logger.info("Instance ID: ${instance.instance_id}")
                logger.info("Instance status: ${instance.status}")

                // After rollback, querying with old instance_id still returns old instance metadata
                // But it should have successor_instance_id pointing to the new instance
                assertEquals(instance.instance_id, instance_id, "Should return the old instance metadata")
                assertEquals(instance.status, "NORMAL", "Instance should be NORMAL after rollback")
                assertEquals(instance.successor_instance_id, new_instance_id, "Should have successor_instance_id pointing to new instance")

                // Verify resource inheritance
                logger.info("Instance obj_info: ${instance.obj_info}")
                assertNotNull(instance.obj_info, "Should have inherited obj_info")

                logger.info("✓ Rollback successful: old instance has successor_instance_id = ${new_instance_id} with inherited resources")
        }

        logger.info("Resource inheritance test completed successfully!")
    }
}
