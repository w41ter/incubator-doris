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

suite("test_clone_extended_operations", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_clone_extended_operations because not in cloud mode")
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

    def wait_alter_table_finish = { cluster, database, table_name ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(1)).atMost(java.time.Duration.ofMinutes(2)).until {
            connectWithDockerCluster(cluster) {
                sql "USE ${database}"
                def res = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName='${table_name}' ORDER BY CreateTime DESC LIMIT 1"
                if (res.size() > 0) {
                    def state = res[0]['State']
                    logger.info("ALTER TABLE ${table_name} state: ${state}")
                    if (state == 'FINISHED') {
                        return true
                    } else if (state == 'CANCELLED') {
                        throw new Exception("ALTER TABLE ${table_name} was cancelled")
                    }
                }
                return false
            }
        }
    }

    def cluster_prefix = "regression_test_clone_extended_ops_"
    def base_name = cluster_prefix + "base"
    def derived_name = cluster_prefix + "derived"

    def base_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "base_instance_id_extended",
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
        instanceId: "derived_instance_id_extended",
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
        // Step 1: Create a snapshot in the base cluster
        String snapshot_id = ""
        connectWithDockerCluster(clusters[base_name]) {
            sql "CREATE DATABASE IF NOT EXISTS test_db_extended"
            sql "USE test_db_extended"
            sql """
                CREATE TABLE IF NOT EXISTS test_operations (
                    id INT,
                    name VARCHAR(100),
                    age INT
                ) UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_operations VALUES (1, 'Alice', 25)"
            sql "INSERT INTO test_operations VALUES (2, 'Bob', 30)"
            sql "INSERT INTO test_operations VALUES (3, 'Charlie', 35)"
            sql "INSERT INTO test_operations VALUES (4, 'David', 40)"

            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_extended_ops')"
            wait_snapshot_completed(clusters[base_name], "snapshot_extended_ops")
            snapshot_id = get_snapshot_id("snapshot_extended_ops")

            logger.info("Base cluster snapshot created with ID: ${snapshot_id}")
        }

        // Step 2: Restore the snapshot in the derived cluster
        def cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id}",
            "from_instance_id": "base_instance_id_extended",
            "instance_id": "derived_instance_id_extended",
            "name": "derived_instance_extended",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_clone_extended_ops",
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

        // Step 3: Test ALTER, UPDATE, DELETE operations on derived cluster
        connectWithDockerCluster(clusters[derived_name]) {
            sql "USE test_db_extended"

            // Verify initial data from snapshot
            def res = sql_return_maparray "SELECT * FROM test_operations ORDER BY id"
            logger.info("Initial data in derived cluster: " + res.toString())
            assertEquals(res.size(), 4)

            // Test 1: ALTER TABLE - Add column
            logger.info("Testing ALTER TABLE ADD COLUMN...")
            sql "ALTER TABLE test_operations ADD COLUMN email VARCHAR(100)"
            // Wait for schema change to complete
            wait_alter_table_finish(clusters[derived_name], "test_db_extended", "test_operations")

            // Verify column added
            def schema = sql_return_maparray "DESC test_operations"
            logger.info("Schema after ADD COLUMN: " + schema.toString())
            def emailColumn = schema.find { it['Field'] == 'email' }
            assertNotNull(emailColumn, "Email column should exist")

            // Test 2: UPDATE operation
            logger.info("Testing UPDATE operation...")
            sql "UPDATE test_operations SET age = 26 WHERE id = 1"
            sql "UPDATE test_operations SET name = 'Bob Updated' WHERE id = 2"

            def updated = sql_return_maparray "SELECT * FROM test_operations WHERE id IN (1, 2) ORDER BY id"
            logger.info("Data after UPDATE: " + updated.toString())
            assertEquals(updated[0]['age'], 26)
            assertEquals(updated[1]['name'], 'Bob Updated')

            // Test 3: DELETE operation
            logger.info("Testing DELETE operation...")
            sql "DELETE FROM test_operations WHERE id = 4"

            def afterDelete = sql_return_maparray "SELECT * FROM test_operations ORDER BY id"
            logger.info("Data after DELETE: " + afterDelete.toString())
            assertEquals(afterDelete.size(), 3)
            def deletedRecord = afterDelete.find { it['id'] == 4 }
            assertNull(deletedRecord, "Record with id=4 should be deleted")

            // Test 4: ALTER TABLE - Drop column
            logger.info("Testing ALTER TABLE DROP COLUMN...")
            sql "ALTER TABLE test_operations DROP COLUMN age"
            // Wait for schema change to complete
            wait_alter_table_finish(clusters[derived_name], "test_db_extended", "test_operations")

            def schemaAfterDrop = sql_return_maparray "DESC test_operations"
            logger.info("Schema after DROP COLUMN: " + schemaAfterDrop.toString())
            def ageColumn = schemaAfterDrop.find { it['Field'] == 'age' }
            assertNull(ageColumn, "Age column should be dropped")

            // Test 5: Verify final state
            def finalData = sql_return_maparray "SELECT * FROM test_operations ORDER BY id"
            logger.info("Final data in derived cluster: " + finalData.toString())
            assertEquals(finalData.size(), 3)
            assertEquals(finalData[0]['id'], 1)
            assertEquals(finalData[0]['name'], 'Alice')
            assertEquals(finalData[1]['id'], 2)
            assertEquals(finalData[1]['name'], 'Bob Updated')
            assertEquals(finalData[2]['id'], 3)
            assertEquals(finalData[2]['name'], 'Charlie')
        }

        // Step 4: Verify base cluster remains unchanged
        connectWithDockerCluster(clusters[base_name]) {
            sql "USE test_db_extended"
            def baseData = sql_return_maparray "SELECT * FROM test_operations ORDER BY id"
            logger.info("Data in base cluster (should be unchanged): " + baseData.toString())
            assertEquals(baseData.size(), 4)
            assertEquals(baseData[0]['age'], 25)  // Original age
            assertEquals(baseData[1]['name'], 'Bob')  // Original name

            def baseSchema = sql_return_maparray "DESC test_operations"
            assertEquals(baseSchema.size(), 3)  // Original 3 columns: id, name, age
        }

        logger.info("All ALTER, UPDATE, DELETE operations tested successfully!")
    }
}
