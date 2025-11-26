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

suite("test_clone_rollback_state_consistency", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_clone_rollback_state_consistency because not in cloud mode")
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

    def cluster_prefix = "regression_test_rollback_state_"
    def base_cluster = cluster_prefix + "base"

    def base_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "instance_state",
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

        // Step 1: Create comprehensive data state and snapshot
        String snapshot_id = ""
        connectWithDockerCluster(clusters[base_cluster]) {
            sql "CREATE DATABASE IF NOT EXISTS test_db_state"
            sql "USE test_db_state"

            // Table 1: User data
            sql """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INT,
                    username VARCHAR(100),
                    email VARCHAR(100),
                    created_at DATETIME
                ) UNIQUE KEY(user_id)
                DISTRIBUTED BY HASH(user_id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO users VALUES (1, 'alice', 'alice@example.com', '2024-01-01 10:00:00')"
            sql "INSERT INTO users VALUES (2, 'bob', 'bob@example.com', '2024-01-01 11:00:00')"
            sql "INSERT INTO users VALUES (3, 'charlie', 'charlie@example.com', '2024-01-01 12:00:00')"

            // Table 2: Orders
            sql """
                CREATE TABLE IF NOT EXISTS orders (
                    order_id INT,
                    user_id INT,
                    amount DECIMAL(10,2),
                    status VARCHAR(50)
                ) UNIQUE KEY(order_id)
                DISTRIBUTED BY HASH(order_id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO orders VALUES (1001, 1, 100.50, 'completed')"
            sql "INSERT INTO orders VALUES (1002, 2, 200.75, 'completed')"
            sql "INSERT INTO orders VALUES (1003, 1, 150.00, 'pending')"

            // Table 3: Partitioned transaction log
            sql """
                CREATE TABLE IF NOT EXISTS transaction_log (
                    txn_id INT,
                    user_id INT,
                    action VARCHAR(100),
                    txn_date DATE
                ) DUPLICATE KEY(txn_id, user_id)
                PARTITION BY RANGE(txn_date) (
                    PARTITION p202401 VALUES LESS THAN ('2024-02-01'),
                    PARTITION p202402 VALUES LESS THAN ('2024-03-01')
                )
                DISTRIBUTED BY HASH(txn_id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO transaction_log VALUES (1, 1, 'login', '2024-01-15')"
            sql "INSERT INTO transaction_log VALUES (2, 2, 'login', '2024-01-16')"
            sql "INSERT INTO transaction_log VALUES (3, 1, 'purchase', '2024-01-17')"
            sql "INSERT INTO transaction_log VALUES (4, 3, 'login', '2024-02-01')"

            // Create snapshot capturing this clean state
            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_clean_state')"
            wait_snapshot_completed(clusters[base_cluster], "snapshot_clean_state")
            snapshot_id = get_snapshot_id("snapshot_clean_state")

            logger.info("Clean state snapshot created with ID: ${snapshot_id}")

            // Simulate various corrupting operations
            logger.info("Simulating data corruption...")

            // Corruption 1: Delete critical user
            sql "DELETE FROM users WHERE user_id = 1"

            // Corruption 2: Update with wrong data
            sql "UPDATE orders SET amount = -999.99, status = 'ERROR' WHERE order_id = 1001"

            // Corruption 3: Insert inconsistent data
            sql "INSERT INTO orders VALUES (9999, 999, 0.00, 'CORRUPTED')"  // non-existent user

            // Corruption 4: Add wrong partition data
            sql "INSERT INTO transaction_log VALUES (999, 999, 'bad_action', '2024-01-20')"

            // Corruption 5: Schema change that breaks things
            sql "ALTER TABLE users ADD COLUMN bad_column INT DEFAULT -1"
            wait_alter_table_finish(clusters[base_cluster], "test_db_state", "users")

            // Verify corrupted state
            def corrupted_users = sql_return_maparray "SELECT * FROM users ORDER BY user_id"
            logger.info("Corrupted users: " + corrupted_users.toString())
            assertEquals(corrupted_users.size(), 2)  // alice deleted

            def corrupted_orders = sql_return_maparray "SELECT * FROM orders WHERE order_id = 1001"
            logger.info("Corrupted order 1001: " + corrupted_orders.toString())
            assertEquals(corrupted_orders[0]['amount'], -999.99)
        }

        // Step 2: Perform rollback using cluster.rollback() method
        logger.info("Step 2: Performing rollback from snapshot...")

        String instance_id = "instance_state"

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

        // Step 3: Comprehensive state consistency verification
        connectWithDockerCluster(clusters[base_cluster]) {
            sql "USE test_db_state"

            logger.info("Test 1: Verifying data consistency - Users table...")
            def users = sql_return_maparray "SELECT * FROM users ORDER BY user_id"
            logger.info("Rollback users data: " + users.toString())
            assertEquals(users.size(), 3)  // All 3 users restored
            assertEquals(users[0]['user_id'], 1)
            assertEquals(users[0]['username'], 'alice')  // Deleted user restored
            assertEquals(users[1]['user_id'], 2)
            assertEquals(users[2]['user_id'], 3)

            logger.info("Test 2: Verifying data consistency - Orders table...")
            def orders = sql_return_maparray "SELECT * FROM orders ORDER BY order_id"
            logger.info("Rollback orders data: " + orders.toString())
            assertEquals(orders.size(), 3)  // Clean orders, no corrupted order

            def order_1001 = orders.find { it['order_id'] == 1001 }
            assertNotNull(order_1001)
            assertEquals(order_1001['amount'], 100.50)  // Original amount restored
            assertEquals(order_1001['status'], 'completed')  // Original status restored

            // Corrupted order should not exist
            def bad_order = orders.find { it['order_id'] == 9999 }
            assertNull(bad_order, "Corrupted order should not exist")

            logger.info("Test 3: Verifying metadata consistency - Schema...")
            def users_schema = sql_return_maparray "DESC users"
            logger.info("Users schema: " + users_schema.toString())
            assertEquals(users_schema.size(), 4)  // id, username, email, created_at (no bad_column)

            def bad_column = users_schema.find { it['Field'] == 'bad_column' }
            assertNull(bad_column, "Bad column should not exist in rollback instance")

            logger.info("Test 4: Verifying partition consistency...")
            def partitions = sql_return_maparray "SHOW PARTITIONS FROM transaction_log"
            logger.info("Partitions: " + partitions.toString())
            assertEquals(partitions.size(), 2)  // p202401, p202402

            def txn_log = sql_return_maparray "SELECT * FROM transaction_log ORDER BY txn_id"
            logger.info("Transaction log: " + txn_log.toString())
            assertEquals(txn_log.size(), 4)  // Clean log entries

            // Bad transaction should not exist
            def bad_txn = txn_log.find { it['txn_id'] == 999 }
            assertNull(bad_txn, "Corrupted transaction should not exist")

            logger.info("Test 5: Verifying referential integrity...")
            // All orders should reference existing users
            def orders_with_users = sql_return_maparray """
                SELECT o.order_id, o.user_id, u.username
                FROM orders o
                LEFT JOIN users u ON o.user_id = u.user_id
                ORDER BY o.order_id
            """
            logger.info("Orders with users: " + orders_with_users.toString())
            assertEquals(orders_with_users.size(), 3)

            // All orders should have matching users
            orders_with_users.each { row ->
                assertNotNull(row['username'], "All orders should reference valid users")
            }

            logger.info("Test 6: Verifying data integrity with aggregations...")
            def user_order_stats = sql_return_maparray """
                SELECT u.user_id, u.username, COUNT(o.order_id) as order_count, SUM(o.amount) as total_amount
                FROM users u
                LEFT JOIN orders o ON u.user_id = o.user_id
                GROUP BY u.user_id, u.username
                ORDER BY u.user_id
            """
            logger.info("User order stats: " + user_order_stats.toString())
            assertEquals(user_order_stats.size(), 3)

            // Verify alice's orders
            def alice_stats = user_order_stats.find { it['user_id'] == 1 }
            assertEquals(alice_stats['order_count'], 2)  // 2 orders
            assertEquals(alice_stats['total_amount'], 250.50)  // 100.50 + 150.00

            logger.info("Test 7: Verifying state consistency across tables...")
            // Count total records
            def users_count = sql_return_maparray "SELECT COUNT(*) as cnt FROM users"
            def orders_count = sql_return_maparray "SELECT COUNT(*) as cnt FROM orders"
            def txn_count = sql_return_maparray "SELECT COUNT(*) as cnt FROM transaction_log"

            logger.info("Record counts - Users: ${users_count[0]['cnt']}, Orders: ${orders_count[0]['cnt']}, Txns: ${txn_count[0]['cnt']}")
            assertEquals(users_count[0]['cnt'], 3)
            assertEquals(orders_count[0]['cnt'], 3)
            assertEquals(txn_count[0]['cnt'], 4)

            logger.info("Test 8: Verifying rollback instance is fully writable...")
            // Should be able to perform normal operations
            sql "INSERT INTO users VALUES (4, 'david', 'david@example.com', '2024-01-02 10:00:00')"
            sql "INSERT INTO orders VALUES (1004, 4, 300.00, 'pending')"
            sql "UPDATE users SET email = 'alice_new@example.com' WHERE user_id = 1"
            sql "DELETE FROM transaction_log WHERE txn_id = 4"

            def final_users = sql_return_maparray "SELECT * FROM users ORDER BY user_id"
            assertEquals(final_users.size(), 4)  // 3 original + 1 new

            def alice_email = final_users.find { it['user_id'] == 1 }
            assertEquals(alice_email['email'], 'alice_new@example.com')
        }

        // Step 4: Verify instance metadata after rollback
        logger.info("Step 4: Verifying instance metadata after rollback...")

        def ms = clusters[base_cluster].getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort

        // Query using the ORIGINAL instance_id (external view)
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/get_instance?token=greedisgood9999&instance_id=${instance_id}"
            check { respCode, body ->
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
                    assertEquals(instance.status, "NORMAL", "Instance should be NORMAL after rollback")
                    assertEquals(instance.successor_instance_id, new_instance_id, "Should have successor_instance_id pointing to new instance")

                    logger.info("✓ Rollback successful: old instance has successor_instance_id = ${new_instance_id}")
                }
            }
        }

        logger.info("State consistency test completed successfully!")
    }
}
