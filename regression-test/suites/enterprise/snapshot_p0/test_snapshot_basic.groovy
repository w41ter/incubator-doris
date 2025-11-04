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

suite("test_snapshot_basic", "snapshot,docker") {
    if (!isCloudMode()) {
        logger.info("Skip test_snapshot_basic because not in cloud mode")
        return
    }

    def wait_snapshot_completed = { snapshot_label ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(1)).atMost(java.time.Duration.ofMinutes(5)).until {
            def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots WHERE LABEL='${snapshot_label}'"
            logger.info("Snapshot ${snapshot_label} status: " + res.toString())
            return res.size() == 1 && res[0]['STATE'] != 'SNAPSHOT_PREPARE'
        }
    }

    def wait_all_snapshot_completed = {
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(1)).atMost(java.time.Duration.ofMinutes(5)).until {
            def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
            logger.info("All snapshot status: " + res.toString())
            for (def r in res) {
                if (r['STATE'] == 'SNAPSHOT_PREPARE') {
                    return false
                }
            }
            return true
        }
    }

    def opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
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

    docker(opt) { ->
        sql "CREATE DATABASE IF NOT EXISTS test_db"
        sql "USE test_db"
        sql """
            CREATE TABLE IF NOT EXISTS test_table (
                id INT,
                name STRING
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        sql "INSERT INTO test_table VALUES (1, 'snapshot_test_data')"

        // case 1. Snapshot is not enabled, should fail
        expectExceptionLike({
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'test_manual_snapshot')"
        }, "failed to begin snapshot, because the snapshot feature is disabled")

        // case 2. Enable snapshot feature, auto snapshot is off by default
        sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"

        def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshot_properties"
        logger.info("Cluster snapshot properties: " + res.toString())
        assertEquals(res.size(), 1)
        assertEquals(res[0]['SNAPSHOT_ENABLED'], 'YES')
        assertFalse(res[0]['AUTO_SNAPSHOT'])

        res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
        assert res.size() == 0

        // case 3. Create manual snapshot, then drop it.
        sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'test_manual_snapshot')"

        wait_snapshot_completed("test_manual_snapshot")

        res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
        logger.info("Cluster snapshots after manual creation: " + res.toString())

        // Drop manual snapshot
        sql "ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '${res[0]['ID']}'"

        res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
        logger.info("Cluster snapshots after dropping manual snapshot: " + res.toString())
        assertEquals(res.size(), 0)

        // case 4. Create another manual snapshot and wait it to recycle
        sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '10', 'label' = 'test_manual_snapshot_2')"
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(1)).atMost(java.time.Duration.ofMinutes(5)).until {
            def res2 = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots WHERE LABEL='test_manual_snapshot_2'"
            return res2.size() > 0
        }

        // It should be recycled automatically after some time
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(1)).atMost(java.time.Duration.ofMinutes(3)).until {
            def res2 = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots WHERE LABEL='test_manual_snapshot_2'"
            logger.info("Snapshot test_manual_snapshot_2 status: " + res2.toString())
            return res2.size() == 0
        }

        // case 5. Enable auto snapshot with max_reserved_snapshots = 2
        sql "ADMIN SET AUTO CLUSTER SNAPSHOT PROPERTIES('max_reserved_snapshots'='2')"

        res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshot_properties"
        logger.info("Cluster snapshot properties after enabling auto snapshot: " + res.toString())
        assertEquals(res.size(), 1)
        assertEquals(res[0]['SNAPSHOT_ENABLED'], 'YES')
        assertTrue(res[0]['AUTO_SNAPSHOT'])
        assertEquals(res[0]['MAX_RESERVED_SNAPSHOTS'], 2)

        // Manual snapshot is supported even if auto snapshot is on
        sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'test_manual_snapshot_3')"
        wait_snapshot_completed("test_manual_snapshot_3")

        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(1)).atMost(java.time.Duration.ofMinutes(5)).until {
            def res2 = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots WHERE LABEL LIKE 'auto_snapshot_%'"
            logger.info("Auto snapshot status: " + res2.toString())
            return res2.size() == 1 && res2[0]['STATE'] != 'SNAPSHOT_PREPARED'
        }

        res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
        logger.info("Cluster snapshots after auto creation: " + res.toString())
        assertTrue(res.any { it['AUTO'] == true && it['STATE'] == 'SNAPSHOT_NORMAL' })

        def auto_snapshot_id = res.find { it['AUTO'] == true }['ID']

        // Drop the auto snapshot
        sql "ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '${auto_snapshot_id}'"
        res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
        logger.info("Cluster snapshots after dropping auto snapshot: " + res.toString())
        assertFalse(res.any { it['AUTO'] == true })
    }
}


