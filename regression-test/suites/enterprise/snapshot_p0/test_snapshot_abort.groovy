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


import groovy.json.JsonSlurper
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.suite.SuiteCluster
import org.awaitility.Awaitility

suite("test_snapshot_abort", "snapshot,docker") {
    if (!isCloudMode()) {
        logger.info("Skip test_snapshot_abort because not in cloud mode")
        return
    }

    def wait_snapshot_completed = { snapshot_label ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(1)).atMost(java.time.Duration.ofMinutes(5)).until {
            def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots WHERE LABEL='${snapshot_label}'"
            logger.info("Snapshot ${snapshot_label} status: " + res.toString())
            return res.size() == 1 && res[0]['STATE'] != 'SNAPSHOT_PREPARE'
        }
    }

    def setDebugPoint = {ip, port, op, name ->
        def urlStr = "http://${ip}:${port}/api/debug_point/${op}/${name}"
        def url = new URL(urlStr)
        def conn = url.openConnection()
        conn.requestMethod = 'POST'
        conn.doOutput = true

        // Add Basic Auth header
        def authString = "root:"
        def encodedAuth = Base64.encoder.encodeToString(authString.getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encodedAuth}")

        // Send empty body (required to trigger POST)
        conn.outputStream.withWriter { it << "" }

        // Read response
        def responseText = conn.inputStream.text
        def json = new JsonSlurper().parseText(responseText)

        return json?.msg == "OK" && json?.code == 0
    }

    def addDebugPoint = { ip, http_port, name ->
        return setDebugPoint(ip, http_port, 'add', name)
    }

    def removeDebugPoint = { ip, http_port, name ->
        return setDebugPoint(ip, http_port, 'remove', name)
    }

    def addFEDebugPoint = { name ->
        def fe = cluster.getMasterFe()
        return addDebugPoint(fe.host, fe.httpPort, name)
    }

    def removeFEDebugPoint = { name ->
        def fe = cluster.getMasterFe()
        return removeDebugPoint(fe.host, fe.httpPort, name)
    }

    def opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        feConfigs: [
            "enable_debug_points=true",
            "cloud_snapshot_timeout_seconds=20"
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
        // Enable snapshot feature, auto snapshot is off by default
        sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"

        // Inject debug point to make upload image fail
        addFEDebugPoint("CloudSnapshotHandler.uploadImage.fail")
        // Create manual snapshot
        sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'test_manual_snapshot')"
        wait_snapshot_completed("test_manual_snapshot")
        // Check snapshot state
        def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
        logger.info("Cluster snapshots after fail: " + res.toString())
        assertEquals(res.size(), 1)
        assertEquals(res[0]['STATE'], 'SNAPSHOT_ABORTED')
        assertEquals(res[0]['MSG'], 'inject CloudSnapshotHandler.uploadImage.fail')
        assertEquals(res[0]['AUTO'], false)
        assertEquals(res[0]['LABEL'], 'test_manual_snapshot')
        assertEquals(res[0]['JOURNAL_ID'], null)
        sql "ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '${res[0]['ID']}'"
        removeFEDebugPoint("CloudSnapshotHandler.uploadImage.fail")

        // Inject debug point to make upload image hang
        addFEDebugPoint("CloudSnapshotHandler.uploadImage.wait")
        // Create manual snapshot
        sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '20', 'label' = 'test_manual_snapshot2')"
        // Restart fe
        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()
        // Check snapshot state
        wait_snapshot_completed("test_manual_snapshot2")
        res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
        logger.info("Cluster snapshots after restart fe: " + res.toString())
        assertEquals(res.size(), 1)
        assertEquals(res[0]['STATE'], 'SNAPSHOT_ABORTED')
    }
}


