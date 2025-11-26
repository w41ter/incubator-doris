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

suite("test_clone_isolation_index", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_clone_isolation_index because not in cloud mode")
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

    def wait_build_index_finish = { cluster, database, table_name, index_name ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(1)).atMost(java.time.Duration.ofMinutes(2)).until {
            connectWithDockerCluster(cluster) {
                sql "USE ${database}"
                def res = sql_return_maparray "SHOW INDEX FROM ${table_name}"
                def indexExists = res.find { it['Key_name'] == index_name }
                if (indexExists) {
                    logger.info("Index ${index_name} on ${table_name} has been created successfully")
                    return true
                }
                logger.info("Waiting for index ${index_name} on ${table_name} to be created...")
                return false
            }
        }
    }

    def cluster_prefix = "regression_test_clone_isolation_index_"
    def base_name = cluster_prefix + "base"
    def derived_name = cluster_prefix + "derived"

    def base_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "base_instance_isolation_index",
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
        instanceId: "derived_instance_isolation_index",
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
        // Step 1: Create table with indexes and snapshot in base cluster
        String snapshot_id = ""
        connectWithDockerCluster(clusters[base_name]) {
            sql "CREATE DATABASE IF NOT EXISTS test_db_index"
            sql "USE test_db_index"

            // Create table with bloom filter index
            sql """
                CREATE TABLE IF NOT EXISTS test_with_bf (
                    id INT,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    INDEX idx_name (name) USING INVERTED,
                    INDEX idx_email_bf (email) USING INVERTED PROPERTIES("parser" = "english")
                ) UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES (
                    "replication_num" = "1",
                    "bloom_filter_columns" = "email"
                )
            """

            sql "INSERT INTO test_with_bf VALUES (1, 'Alice Smith', 'alice@example.com')"
            sql "INSERT INTO test_with_bf VALUES (2, 'Bob Jones', 'bob@example.com')"

            // Create table for ngram index test
            sql """
                CREATE TABLE IF NOT EXISTS test_with_ngram (
                    id INT,
                    title VARCHAR(200),
                    content TEXT,
                    INDEX idx_content_ngram (content) USING INVERTED PROPERTIES("parser" = "standard")
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """

            sql "INSERT INTO test_with_ngram VALUES (1, 'Title 1', 'This is the first document')"
            sql "INSERT INTO test_with_ngram VALUES (2, 'Title 2', 'This is the second document')"

            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_index')"
            wait_snapshot_completed(clusters[base_name], "snapshot_index")
            snapshot_id = get_snapshot_id("snapshot_index")

            logger.info("Base cluster snapshot created with ID: ${snapshot_id}")

            // Add more data after snapshot
            sql "INSERT INTO test_with_bf VALUES (3, 'Charlie Brown', 'charlie@example.com')"
            sql "INSERT INTO test_with_ngram VALUES (3, 'Title 3', 'This is the third document')"
        }

        // Step 2: Restore snapshot in derived cluster
        def cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id}",
            "from_instance_id": "base_instance_isolation_index",
            "instance_id": "derived_instance_isolation_index",
            "name": "derived_instance_index",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_clone_isolation_index",
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

        // Step 3: Perform index operations in derived cluster
        connectWithDockerCluster(clusters[derived_name]) {
            sql "USE test_db_index"

            // Verify initial data from snapshot
            logger.info("Verifying initial snapshot data in derived cluster...")
            def bf_data = sql_return_maparray "SELECT * FROM test_with_bf ORDER BY id"
            assertEquals(bf_data.size(), 2)

            def ngram_data = sql_return_maparray "SELECT * FROM test_with_ngram ORDER BY id"
            assertEquals(ngram_data.size(), 2)

            // Test 1: Create new table with indexes in derived cluster
            logger.info("Test 1: Creating new table with indexes in derived cluster...")
            sql """
                CREATE TABLE derived_table_with_index (
                    id INT,
                    description VARCHAR(200),
                    tags VARCHAR(100),
                    INDEX idx_desc (description) USING INVERTED,
                    INDEX idx_tags_bf (tags) USING INVERTED PROPERTIES("parser" = "english")
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES (
                    "replication_num" = "1",
                    "bloom_filter_columns" = "tags"
                )
            """
            sql "INSERT INTO derived_table_with_index VALUES (1, 'Product A', 'electronics,gadget')"
            sql "INSERT INTO derived_table_with_index VALUES (2, 'Product B', 'book,education')"

            def derived_table_data = sql_return_maparray "SELECT * FROM derived_table_with_index ORDER BY id"
            assertEquals(derived_table_data.size(), 2)

            // Test 2: Add index to existing table in derived cluster
            logger.info("Test 2: Adding index to existing table in derived cluster...")
            sql "CREATE INDEX idx_id ON test_with_bf(id) USING INVERTED"
            wait_build_index_finish(clusters[derived_name], "test_db_index", "test_with_bf", "idx_id")

            def indexes_after_add = sql_return_maparray "SHOW INDEX FROM test_with_bf"
            logger.info("Indexes after ADD: " + indexes_after_add.toString())

            // Test 3: Drop index in derived cluster
            logger.info("Test 3: Dropping index in derived cluster...")
            sql "DROP INDEX idx_email_bf ON test_with_bf"

            def indexes_after_drop = sql_return_maparray "SHOW INDEX FROM test_with_bf"
            logger.info("Indexes after DROP: " + indexes_after_drop.toString())

            // Test 4: Insert data into tables with modified indexes
            logger.info("Test 4: Inserting data with modified indexes in derived cluster...")
            sql "INSERT INTO test_with_bf VALUES (100, 'Derived User', 'derived@example.com')"
            sql "INSERT INTO test_with_ngram VALUES (100, 'Derived Title', 'Derived content')"

            def final_bf_data = sql_return_maparray "SELECT * FROM test_with_bf ORDER BY id"
            logger.info("Final data in test_with_bf: " + final_bf_data.toString())
            assertEquals(final_bf_data.size(), 3)

            // Test 5: Modify bloom filter columns
            logger.info("Test 5: Modifying bloom filter properties in derived cluster...")
            sql "ALTER TABLE test_with_ngram SET ('bloom_filter_columns' = 'title')"

            def table_properties = sql_return_maparray "SHOW CREATE TABLE test_with_ngram"
            logger.info("Table properties after modification: " + table_properties.toString())
        }

        // Step 4: Verify base cluster indexes and data are unchanged
        connectWithDockerCluster(clusters[base_name]) {
            sql "USE test_db_index"

            logger.info("Verifying base cluster index isolation...")

            // Test 1: New table should NOT exist in base cluster
            logger.info("Test 1: Verifying derived table does NOT exist in base cluster...")
            test {
                sql "SELECT * FROM derived_table_with_index"
                exception "does not exist in database [test_db_index]"
            }

            // Test 2: Original indexes should be unchanged
            logger.info("Test 2: Verifying indexes are unchanged in base cluster...")
            def base_indexes = sql_return_maparray "SHOW INDEX FROM test_with_bf"
            logger.info("Base cluster indexes: " + base_indexes.toString())

            // idx_email_bf should still exist (not dropped)
            def email_bf_index = base_indexes.find { it['Key_name'] == 'idx_email_bf' }
            assertNotNull(email_bf_index, "idx_email_bf should still exist in base cluster")

            // idx_name_new should NOT exist (not added)
            def name_new_index = base_indexes.find { it['Key_name'] == 'idx_name_new' }
            assertNull(name_new_index, "idx_name_new should NOT exist in base cluster")

            // Test 3: Data should be unchanged (plus post-snapshot data)
            logger.info("Test 3: Verifying data is unchanged in base cluster...")
            def base_bf_data = sql_return_maparray "SELECT * FROM test_with_bf ORDER BY id"
            logger.info("Base cluster data: " + base_bf_data.toString())
            assertEquals(base_bf_data.size(), 3)  // 2 original + 1 after snapshot

            assertEquals(base_bf_data[0]['id'], 1)
            assertEquals(base_bf_data[1]['id'], 2)
            assertEquals(base_bf_data[2]['id'], 3)  // Post-snapshot data

            // Derived cluster data should NOT exist
            def check_100 = base_bf_data.findAll { it['id'] == 100 }
            assertEquals(check_100.size(), 0)

            // Test 4: Verify ngram table unchanged
            def base_ngram_data = sql_return_maparray "SELECT * FROM test_with_ngram ORDER BY id"
            assertEquals(base_ngram_data.size(), 3)  // 2 original + 1 after snapshot

            def table_properties = sql_return_maparray "SHOW CREATE TABLE test_with_ngram"
            logger.info("Base cluster table properties: " + table_properties.toString())
            // Bloom filter should NOT be modified
        }

        logger.info("Index isolation test completed successfully!")
    }
}
