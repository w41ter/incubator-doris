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

suite("test_clone_mow", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_clone_mow because not in cloud mode")
        return
    }

    def wait_snapshot_completed = { cluster, snapshot_label ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(1)).atMost(java.time.Duration.ofMinutes(5)).until {
            connectWithDockerCluster(cluster) {
                def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots WHERE LABEL='${snapshot_label}'"
                logger.info("Snapshot ${snapshot_label} status: " + res.toString())
                return res.size() == 1 && res[0]['STATE'] != 'SNAPSHOT_PREPARE'
            }
        }
    }

    def get_snapshot_id = { snapshot_label ->
        def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots WHERE LABEL='${snapshot_label}'"
        assertEquals(res.size(), 1)
        return res[0]['ID']
    }

    def cluster_prefix = "regression_test_clone_mow_"
    def base_name = cluster_prefix + "base"
    def derived_name = cluster_prefix + "derived"

    def base_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "base_instance_id",
        feConfigs: [
            "sys_log_verbose_modules=org.apache.doris.journal"
        ],
        beConfigs: [
            "delete_bitmap_store_write_version=2",
            "delete_bitmap_store_read_version=2",
            "delete_bitmap_store_v2_max_bytes_in_fdb=0"
        ],
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
        ])

    def derive_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 0,
        instanceId: "derived_instance_id",
        externalMsCluster: base_name,
        beConfigs: [
            "delete_bitmap_store_write_version=2",
            "delete_bitmap_store_read_version=2",
            "delete_bitmap_store_v2_max_bytes_in_fdb=0"
        ],
        msConfigs: [
            "enable_split_rowset_meta=true",
            "enable_split_tablet_schema_pb=true",
            "enable_multi_version_status=true",
            "enable_snapshot_data_migrator=true",
        ])

    def manual_init_clusters = [derived_name].toSet()
    dockers(["${base_name}": base_opt, "${derived_name}": derive_opt], manual_init_clusters) { clusters ->
        // Step 1: Create a snapshot in the base cluster
        String snapshot_id = ""
        connectWithDockerCluster(clusters[base_name]) {
            sql "CREATE DATABASE IF NOT EXISTS test_db"
            sql "USE test_db"
            sql """
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT,
                    name VARCHAR(100)
                ) UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
            """
            sql "INSERT INTO test_table VALUES (1, 'cluster0_data')"
            sql "INSERT INTO test_table VALUES (2, 'cluster2_data')"
            sql "INSERT INTO test_table VALUES (1, 'cluster1_data')"
            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_label')"
            wait_snapshot_completed(clusters[base_name], "snapshot_label")
            snapshot_id = get_snapshot_id("snapshot_label")

            // Below inserts are not included in the snapshot
            sql "INSERT INTO test_table VALUES (3, 'cluster1_new_data')"
            sql "INSERT INTO test_table VALUES (4, 'cluster2_new_data')"
            sql "INSERT INTO test_table VALUES (2, 'cluster2_new_data')"
        }

        // Step 2: Restore the snapshot in the derived cluster
        def cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id}",
            "from_instance_id": "base_instance_id",
            "instance_id": "derived_instance_id",
            "name": "derived_instance",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_clone_mow",
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

        connectWithDockerCluster(clusters[derived_name]) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data in derived cluster after clone: " + res.toString())
            assertEquals(res.size(), 2)
            assertEquals(res[0]['id'], 1)
            assertEquals(res[0]['name'], 'cluster1_data')
            assertEquals(res[1]['id'], 2)
            assertEquals(res[1]['name'], 'cluster2_data')

            // Below inserts are not included in the base cluster
            sql "INSERT INTO test_table VALUES (3, 'derived_cluster1_new_data')"
            sql "INSERT INTO test_table VALUES (4, 'derived_cluster2_new_data')"
            sql "INSERT INTO test_table VALUES (2, 'derived_cluster2_new_data')"
        }

        connectWithDockerCluster(clusters[base_name]) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data in base cluster after clone: " + res.toString())
            assertEquals(res.size(), 4)
            assertEquals(res[0]['id'], 1)
            assertEquals(res[0]['name'], 'cluster1_data')
            assertEquals(res[1]['id'], 2)
            assertEquals(res[1]['name'], 'cluster2_new_data')
            assertEquals(res[2]['id'], 3)
            assertEquals(res[2]['name'], 'cluster1_new_data')
            assertEquals(res[3]['id'], 4)
            assertEquals(res[3]['name'], 'cluster2_new_data')
        }
    }
}

