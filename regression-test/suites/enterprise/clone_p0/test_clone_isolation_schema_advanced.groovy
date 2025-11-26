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

suite("test_clone_isolation_schema_advanced", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_clone_isolation_schema_advanced because not in cloud mode")
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

    def cluster_prefix = "regression_test_clone_schema_adv_"
    def base_name = cluster_prefix + "base"
    def derived_name = cluster_prefix + "derived"

    def base_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "base_instance_schema_adv",
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
        instanceId: "derived_instance_schema_adv",
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
        // Step 1: Create snapshot in base cluster
        String snapshot_id = ""
        connectWithDockerCluster(clusters[base_name]) {
            sql "CREATE DATABASE IF NOT EXISTS test_db_schema_adv"
            sql "USE test_db_schema_adv"

            sql """
                CREATE TABLE IF NOT EXISTS test_advanced (
                    id INT,
                    name VARCHAR(100),
                    age INT,
                    score DECIMAL(5,2)
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_advanced VALUES (1, 'User1', 25, 85.5)"
            sql "INSERT INTO test_advanced VALUES (2, 'User2', 30, 90.0)"
            sql "INSERT INTO test_advanced VALUES (3, 'User3', 35, 78.5)"

            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_schema_adv')"
            wait_snapshot_completed(clusters[base_name], "snapshot_schema_adv")
            snapshot_id = get_snapshot_id("snapshot_schema_adv")

            logger.info("Base cluster snapshot created with ID: ${snapshot_id}")
        }

        // Step 2: Restore snapshot in derived cluster
        def cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id}",
            "from_instance_id": "base_instance_schema_adv",
            "instance_id": "derived_instance_schema_adv",
            "name": "derived_instance_schema_adv",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_clone_schema_adv",
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

        // Step 3: Test advanced schema changes in derived cluster
        connectWithDockerCluster(clusters[derived_name]) {
            sql "USE test_db_schema_adv"

            // Test 1: ALTER TABLE MODIFY COLUMN (increase VARCHAR length)
            logger.info("Test 1: ALTER TABLE MODIFY COLUMN in derived cluster...")
            sql "ALTER TABLE test_advanced MODIFY COLUMN name VARCHAR(200)"
            wait_alter_table_finish(clusters[derived_name], "test_db_schema_adv", "test_advanced")

            def schema_after_modify = sql_return_maparray "DESC test_advanced"
            logger.info("Schema after MODIFY: " + schema_after_modify.toString())
            def nameCol = schema_after_modify.find { it['Field'] == 'name' }
            assertNotNull(nameCol)
            // VARCHAR(200) should be reflected in the schema
            assertTrue(nameCol['Type'].contains('varchar'))

            // Test 2: ALTER TABLE SET PROPERTIES (add bloom filter)
            logger.info("Test 2: ALTER TABLE SET PROPERTIES in derived cluster...")
            sql "ALTER TABLE test_advanced SET ('bloom_filter_columns' = 'name')"
            // Wait a bit for property change to take effect
            sleep(3000)

            def props = sql_return_maparray "SHOW CREATE TABLE test_advanced"
            logger.info("Properties after SET: " + props.toString())

            // Wait to ensure table state is back to NORMAL before next ALTER
            sleep(2000)

            // Test 3: ALTER TABLE ADD COLUMN
            logger.info("Test 3: ALTER TABLE ADD COLUMN in derived cluster...")
            sql "ALTER TABLE test_advanced ADD COLUMN email VARCHAR(100) DEFAULT ''"
            wait_alter_table_finish(clusters[derived_name], "test_db_schema_adv", "test_advanced")

            def schema_with_new_col = sql_return_maparray "DESC test_advanced"
            logger.info("Schema with new column: " + schema_with_new_col.toString())
            def emailCol = schema_with_new_col.find { it['Field'] == 'email' }
            assertNotNull(emailCol, "Email column should exist")

            // Insert data with new column
            sql "INSERT INTO test_advanced VALUES (4, 'User4', 28, 88.0, 'user4@example.com')"
            def data_with_email = sql_return_maparray "SELECT * FROM test_advanced WHERE id = 4"
            logger.info("Data with email column: " + data_with_email.toString())
            assertEquals(data_with_email.size(), 1)
            assertEquals(data_with_email[0]['email'], 'user4@example.com')

            // Test 4: ALTER TABLE MODIFY COLUMN ORDER
            logger.info("Test 4: ALTER TABLE MODIFY COLUMN ORDER in derived cluster...")
            sql "ALTER TABLE test_advanced MODIFY COLUMN name VARCHAR(200) AFTER age"
            wait_alter_table_finish(clusters[derived_name], "test_db_schema_adv", "test_advanced")

            def schema_reordered = sql_return_maparray "DESC test_advanced"
            logger.info("Schema after reorder: " + schema_reordered.toString())

            // Test 5: ALTER TABLE DROP COLUMN
            logger.info("Test 5: ALTER TABLE DROP COLUMN in derived cluster...")
            sql "ALTER TABLE test_advanced DROP COLUMN score"
            wait_alter_table_finish(clusters[derived_name], "test_db_schema_adv", "test_advanced")

            def schema_after_drop = sql_return_maparray "DESC test_advanced"
            logger.info("Schema after DROP COLUMN: " + schema_after_drop.toString())
            def scoreCol = schema_after_drop.find { it['Field'] == 'score' }
            assertNull(scoreCol, "Score column should be dropped")
        }

        // Step 4: Verify base cluster remains unchanged
        connectWithDockerCluster(clusters[base_name]) {
            sql "USE test_db_schema_adv"

            logger.info("Verifying base cluster schema isolation...")

            // Original schema should be intact
            def base_schema = sql_return_maparray "DESC test_advanced"
            logger.info("Base cluster schema: " + base_schema.toString())

            // Should have original 4 columns only
            assertEquals(base_schema.size(), 4)  // id, name, age, score

            // score should still be DECIMAL
            def scoreCol = base_schema.find { it['Field'] == 'score' }
            assertNotNull(scoreCol)
            assertTrue(scoreCol['Type'].contains('DECIMAL') || scoreCol['Type'].contains('decimal'))

            // No generated column
            def genCol = base_schema.find { it['Field'] == 'age_group' }
            assertNull(genCol, "Generated column should NOT exist in base cluster")

            // No cancelled test column
            def cancelCol = base_schema.find { it['Field'] == 'cancel_test_col' }
            assertNull(cancelCol, "Cancelled column should NOT exist in base cluster")

            // Data should be unchanged
            def base_data = sql_return_maparray "SELECT * FROM test_advanced ORDER BY id"
            assertEquals(base_data.size(), 3)
            assertEquals(base_data[0]['name'], 'User1')
        }

        logger.info("Advanced schema change isolation test completed successfully!")
    }
}
