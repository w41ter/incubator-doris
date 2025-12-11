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

suite("test_clone_dynamic_partition", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_clone_dynamic_partition because not in cloud mode")
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

    def wait_for_partition_num = { cluster, database, table_name, expected_num ->
        for (int i = 0; i < 40; i++) {
            def res = null
            connectWithDockerCluster(cluster) {
                sql "USE ${database}"
                res = sql "show partitions from ${table_name}"
            }
            if (res.size() == expected_num) {
                break
            } else {
                sleep(2000)
            }
        }
        def res = null
        connectWithDockerCluster(cluster) {
            sql "USE ${database}"
            res = sql "show partitions from ${table_name}"
        }
        logger.info("partitions: " + res.toString())
        assertEquals(res.size(), expected_num)
    }

    def drop_catalog_recycle_bin = {
        def res = sql_return_maparray "show catalog recycle bin"
        for (int i = 0; i < res.size(); i++) {
            if (res[i]['PartitionId'] != '') {
                sql "drop catalog recycle bin where 'partitionId' = ${res[i]['PartitionId']} "
            }
        }
        res = sql_return_maparray "show catalog recycle bin"
        logger.info("catalog recycle bin: " + res.toString())
        assertEquals(res.size(), 0)
    }

    def cluster_prefix = "regression_test_clone_dynamic_partition_"
    def base_name = cluster_prefix + "base"
    def derived_name = cluster_prefix + "derived"

    def base_opt = new ClusterOptions(
            cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
            instanceId: "base_instance_id",
            feConfigs: [
                    "enable_debug_points=true",
                    "dynamic_partition_check_interval_seconds=2",
                    "catalog_trash_expire_second=1"
            ],
            beConfigs: [
                "delete_bitmap_store_write_version=3",
                "delete_bitmap_store_read_version=3",
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
            ])

    def derive_opt = new ClusterOptions(
            cloudMode: true, feNum: 1, beNum: 1, msNum: 0,
            instanceId: "derived_instance_id",
            externalMsCluster: base_name,
            beConfigs: [
                "delete_bitmap_store_write_version=3",
                "delete_bitmap_store_read_version=3",
            ],
            feConfigs: [
                    "enable_debug_points=true",
                    "dynamic_partition_check_interval_seconds=2",
                    "catalog_trash_expire_second=1",
                    "dynamic_partition_enable=false"
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
                CREATE TABLE test_table ( k1 DATE NOT NULL, k2 varchar(20) NOT NULL)
                PARTITION BY RANGE(k1)
                (
                    PARTITION `p20250801` VALUES [("2025-08-01"),  ("2025-08-02")),
                    PARTITION `p20250901` VALUES [("2025-09-01"),  ("2025-09-02")),
                    PARTITION `p20251001` VALUES [("2025-10-01"),  ("2025-10-02"))
                ) DISTRIBUTED BY HASH(k1) BUCKETS 2;
            """
            sql "INSERT INTO test_table VALUES ('2025-08-01', '2025-08-01-val_1')"
            sql "INSERT INTO test_table VALUES ('2025-08-01', '2025-08-01-val_2')"
            sql "INSERT INTO test_table VALUES ('2025-09-01', '2025-09-01-val_1')"
            sql "INSERT INTO test_table VALUES ('2025-09-01', '2025-09-01-val_2')"
            sql "INSERT INTO test_table VALUES ('2025-10-01', '2025-10-01-val_1')"
            sql "INSERT INTO test_table VALUES ('2025-10-01', '2025-10-01-val_2')"
            def res = sql "show partitions from test_table"
            assertEquals(res.size(), 3)
            res = sql "select * from test_table"
            assertEquals(res.size(), 6)

            // alter table to dynamic partition and reserve 2 history partitions
            sql """ ALTER TABLE test_table SET (
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "DAY",
                "dynamic_partition.start" = "-2",
                "dynamic_partition.end" = "3",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.create_history_partition" = "true",
                "dynamic_partition.reserved_history_periods"="[2025-08-01,2025-08-02],[2025-09-01,2025-09-02]"
            );
            """

            // wait for 6 dynamic partitions to be created and 1 history partition to be dropped
            wait_for_partition_num(clusters[base_name], "test_db", "test_table", 8)
            res = sql "select * from test_table"
            assertEquals(res.size(), 4)
            drop_catalog_recycle_bin()

            // temporarily disable dynamic partition scheduler
            sql """ ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "false") """

            // alter table to [-1, 5] range
            sql """ ALTER TABLE test_table SET (
                "dynamic_partition.start" = "-1",
                "dynamic_partition.end" = "5"
            );
            """
            res = sql "show partitions from test_table"
            assertEquals(res.size(), 8)

            // create snapshot
            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_label')"
            wait_snapshot_completed(clusters[base_name], "snapshot_label")
            snapshot_id = get_snapshot_id("snapshot_label")

            res = sql "show partitions from test_table"
            assertEquals(res.size(), 8)

            // enable dynamic partition scheduler
            sql """ ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true") """
            // wait for 1 history partition to be dropped, 2 new partitions to be created
            wait_for_partition_num(clusters[base_name], "test_db", "test_table", 9)
            res = sql "select * from test_table"
            assertEquals(res.size(), 4)
            drop_catalog_recycle_bin()

            // alter table to drop 1 history partition
            sql """ ALTER TABLE test_table SET ("dynamic_partition.reserved_history_periods"="[2025-08-01,2025-08-02]");"""
            // wait for 1 history partition to be dropped
            wait_for_partition_num(clusters[base_name], "test_db", "test_table", 8)
            res = sql "select * from test_table"
            assertEquals(res.size(), 2)
            drop_catalog_recycle_bin()
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
                "prefix": "regression_test_clone_dynamic_partition",
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

            // dynamic partition scheduler is disabled, load 8 partitions from snapshot
            def res = sql "show partitions from test_table"
            assertEquals(res.size(), 8)

            // enable dynamic partition scheduler
            sql """ ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true") """

            // check 9 partitions: create 2 partitions and drop 1 history partition
            wait_for_partition_num(clusters[derived_name], "test_db", "test_table", 9)
            res = sql_return_maparray "SELECT * FROM test_table"
            logger.info("Data in derived cluster after clone: " + res.toString())
            assertEquals(res.size(), 4)
            res = sql "show partitions from test_table"
            assertEquals(res.size(), 9)

            // alter table to drop 1 history partition
            sql """ ALTER TABLE test_table SET ("dynamic_partition.reserved_history_periods"="[2025-09-01,2025-09-02]");"""
            wait_for_partition_num(clusters[derived_name], "test_db", "test_table", 8)
            res = sql "select * from test_table"
            assertEquals(res.size(), 2)
            assertEquals(res[0][0].toString(), '2025-09-01')
            drop_catalog_recycle_bin()
        }

        connectWithDockerCluster(clusters[base_name]) {
            sql "USE test_db"
            def res = sql "SELECT * FROM test_table"
            logger.info("Data in base cluster after clone: " + res.toString())
            assertEquals(res.size(), 2)
            assertEquals(res[0][0].toString(), '2025-08-01')
        }
    }
}

