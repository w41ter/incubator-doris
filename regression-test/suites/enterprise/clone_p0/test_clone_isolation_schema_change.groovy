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

suite("test_clone_isolation_schema_change", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_clone_isolation_schema_change because not in cloud mode")
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

    def cluster_prefix = "regression_test_clone_isolation_schema_"
    def base_name = cluster_prefix + "base"
    def derived_name = cluster_prefix + "derived"

    def base_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "base_instance_isolation_schema",
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
        instanceId: "derived_instance_isolation_schema",
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
            sql "CREATE DATABASE IF NOT EXISTS test_db_schema_change"
            sql "USE test_db_schema_change"

            sql """
                CREATE TABLE IF NOT EXISTS test_schema (
                    id INT,
                    name VARCHAR(100),
                    age INT
                ) UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_schema VALUES (1, 'User1', 25)"
            sql "INSERT INTO test_schema VALUES (2, 'User2', 30)"

            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_schema_change')"
            wait_snapshot_completed(clusters[base_name], "snapshot_schema_change")
            snapshot_id = get_snapshot_id("snapshot_schema_change")

            logger.info("Base cluster snapshot created with ID: ${snapshot_id}")
        }

        // Step 2: Restore snapshot in derived cluster
        def cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id}",
            "from_instance_id": "base_instance_isolation_schema",
            "instance_id": "derived_instance_isolation_schema",
            "name": "derived_instance_schema",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_clone_isolation_schema",
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

        // Step 3: Perform various schema changes in derived cluster
        connectWithDockerCluster(clusters[derived_name]) {
            sql "USE test_db_schema_change"

            // Test 1: ALTER TABLE ADD COLUMN
            logger.info("Test 1: ALTER TABLE ADD COLUMN in derived cluster...")
            sql "ALTER TABLE test_schema ADD COLUMN email VARCHAR(100)"
            wait_alter_table_finish(clusters[derived_name], "test_db_schema_change", "test_schema")

            def schema_after_add = sql_return_maparray "DESC test_schema"
            logger.info("Schema after ADD COLUMN: " + schema_after_add.toString())
            def emailColumn = schema_after_add.find { it['Field'] == 'email' }
            assertNotNull(emailColumn, "Email column should exist in derived cluster")

            // Test 2: ALTER TABLE DROP COLUMN
            logger.info("Test 2: ALTER TABLE DROP COLUMN in derived cluster...")
            sql "ALTER TABLE test_schema DROP COLUMN age"
            wait_alter_table_finish(clusters[derived_name], "test_db_schema_change", "test_schema")

            def schema_after_drop = sql_return_maparray "DESC test_schema"
            logger.info("Schema after DROP COLUMN: " + schema_after_drop.toString())
            def ageColumn = schema_after_drop.find { it['Field'] == 'age' }
            assertNull(ageColumn, "Age column should be dropped in derived cluster")

            // Test 3: ALTER TABLE RENAME
            logger.info("Test 3: ALTER TABLE RENAME in derived cluster...")
            sql "ALTER TABLE test_schema RENAME test_schema_renamed"

            def tables = sql_return_maparray "SHOW TABLES"
            logger.info("Tables after RENAME: " + tables.toString())
            def renamedTable = tables.find { it['Tables_in_test_db_schema_change'] == 'test_schema_renamed' }
            assertNotNull(renamedTable, "Renamed table should exist")

            // Test 4: ALTER TABLE MODIFY COLUMN
            logger.info("Test 4: ALTER TABLE MODIFY COLUMN in derived cluster...")
            sql "ALTER TABLE test_schema_renamed MODIFY COLUMN name VARCHAR(200)"
            wait_alter_table_finish(clusters[derived_name], "test_db_schema_change", "test_schema_renamed")

            def schema_after_modify = sql_return_maparray "DESC test_schema_renamed"
            logger.info("Schema after MODIFY COLUMN: " + schema_after_modify.toString())
            def nameColumn = schema_after_modify.find { it['Field'] == 'name' }
            assertNotNull(nameColumn, "Name column should exist")

            // Test 5: ALTER TABLE REPLACE (create a new table first)
            logger.info("Test 5: ALTER TABLE REPLACE in derived cluster...")
            sql """
                CREATE TABLE test_schema_new (
                    id INT,
                    name VARCHAR(100),
                    phone VARCHAR(20)
                ) UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_schema_new VALUES (10, 'NewUser10', '1234567890')"

            sql "ALTER TABLE test_schema_renamed REPLACE WITH TABLE test_schema_new"
            wait_alter_table_finish(clusters[derived_name], "test_db_schema_change", "test_schema_renamed")

            def replaced_data = sql_return_maparray "SELECT * FROM test_schema_renamed ORDER BY id"
            logger.info("Data after REPLACE: " + replaced_data.toString())
            assertEquals(replaced_data.size(), 1)
            assertEquals(replaced_data[0]['id'], 10)

            // Test 6: ALTER TABLE MODIFY COMMENT
            logger.info("Test 6: ALTER TABLE MODIFY COMMENT in derived cluster...")
            sql "ALTER TABLE test_schema_renamed MODIFY COMMENT 'Modified in derived cluster'"
        }

        // Step 4: Verify base cluster schema is completely unchanged
        connectWithDockerCluster(clusters[base_name]) {
            sql "USE test_db_schema_change"

            logger.info("Verifying base cluster schema isolation...")

            // Test 1: Original table name should still exist
            logger.info("Test 1: Verifying original table name exists in base cluster...")
            def tables = sql_return_maparray "SHOW TABLES"
            logger.info("Tables in base cluster: " + tables.toString())
            def originalTable = tables.find { it['Tables_in_test_db_schema_change'] == 'test_schema' }
            assertNotNull(originalTable, "Original table name 'test_schema' should exist in base cluster")

            // Test 2: Schema should be unchanged (no email column, age column still exists)
            logger.info("Test 2: Verifying schema is unchanged in base cluster...")
            def base_schema = sql_return_maparray "DESC test_schema"
            logger.info("Base cluster schema: " + base_schema.toString())

            def emailColumn = base_schema.find { it['Field'] == 'email' }
            assertNull(emailColumn, "Email column should NOT exist in base cluster")

            def ageColumn = base_schema.find { it['Field'] == 'age' }
            assertNotNull(ageColumn, "Age column should still exist in base cluster")

            assertEquals(base_schema.size(), 3)  // id, name, age (original schema)

            // Test 3: Data should be unchanged
            logger.info("Test 3: Verifying data is unchanged in base cluster...")
            def base_data = sql_return_maparray "SELECT * FROM test_schema ORDER BY id"
            logger.info("Base cluster data: " + base_data.toString())
            assertEquals(base_data.size(), 2)
            assertEquals(base_data[0]['id'], 1)
            assertEquals(base_data[0]['name'], 'User1')
            assertEquals(base_data[0]['age'], 25)
            assertEquals(base_data[1]['id'], 2)
            assertEquals(base_data[1]['name'], 'User2')
            assertEquals(base_data[1]['age'], 30)

            // Test 4: Renamed table should NOT exist
            logger.info("Test 4: Verifying renamed table does NOT exist in base cluster...")
            test {
                sql "SELECT * FROM test_schema_renamed"
                exception "does not exist in database [test_db_schema_change]"
            }
        }

        logger.info("Schema change isolation test completed successfully!")
    }
}
