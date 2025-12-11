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

suite("test_rollback_chain", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_rollback_chain because not in cloud mode")
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

    def get_instance_api = { msHttpPort, instance_id, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/get_instance?token=greedisgood9999&instance_id=${instance_id}"
            check check_func
        }
    }

    def cluster_prefix = "regression_test_rollback_chain_"
    def instance1_name = cluster_prefix + "instance1"
    def instance4_name = cluster_prefix + "instance4"
    def instance5_name = cluster_prefix + "instance5"

    def opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "instance1",
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
    def instance4_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 0,
        instanceId: "instance4",
        externalMsCluster: instance1_name,
        msConfigs: [
            "enable_split_rowset_meta=true",
            "enable_split_tablet_schema_pb=true",
            "enable_multi_version_status=true",
            "enable_snapshot_data_migrator=true",
        ])
    def instance5_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 0,
        instanceId: "instance5",
        externalMsCluster: instance1_name,
        msConfigs: [
            "enable_split_rowset_meta=true",
            "enable_split_tablet_schema_pb=true",
            "enable_multi_version_status=true",
            "enable_snapshot_data_migrator=true",
        ])

    def manual_init_clusters = [instance4_name, instance5_name].toSet()
    dockers(["${instance1_name}": opt, "${instance4_name}": instance4_opt, "${instance5_name}": instance5_opt], manual_init_clusters) { clusters ->
        // Step 1: Create a snapshot
        String snapshot_id_1_1 = ""
        String snapshot_id_1_2 = ""
        String snapshot_id_1_3 = ""
        connectWithDockerCluster(clusters[instance1_name]) {
            sql "CREATE DATABASE IF NOT EXISTS test_db"
            sql "USE test_db"
            sql """
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT,
                    name VARCHAR(100)
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_table VALUES (1, '1_s_1_1')"
            sql "INSERT INTO test_table VALUES (2, '2_s_1_1')"
            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_1_1')"
            wait_snapshot_completed(clusters[instance1_name], "snapshot_1_1")
            snapshot_id_1_1 = get_snapshot_id("snapshot_1_1")

            sql "INSERT INTO test_table VALUES (3, '3_s_1_2')"
            sql "INSERT INTO test_table VALUES (4, '4_s_1_2')"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_1_2')"
            wait_snapshot_completed(clusters[instance1_name], "snapshot_1_2")
            snapshot_id_1_2 = get_snapshot_id("snapshot_1_2")

            sql "INSERT INTO test_table VALUES (5, '5_s_1_3')"
            sql "INSERT INTO test_table VALUES (6, '6_s_1_3')"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_1_3')"
            wait_snapshot_completed(clusters[instance1_name], "snapshot_1_3")
            snapshot_id_1_3 = get_snapshot_id("snapshot_1_3")

            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data in snapshot_1_3: " + res.toString())
            assertEquals(res.size(), 6)

            sql "INSERT INTO test_table VALUES (7, '7_i_1')"
            sql "INSERT INTO test_table VALUES (8, '8_i_1')"

            res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
            assertEquals(res.size(), 3)
        }

        // Step 2: Rollback instance1 to instance2: using snapshot_1_2
        def cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id_1_2}",
            "from_instance_id": "instance1",
            "instance_id": "instance2",
            "name": "instance2_name",
            "is_successor": true
        }
        """
        logger.info("Rollback the cluster with snapshot: " + cluster_snapshot_content)
        clusters[instance1_name].rollback(cluster_snapshot_content)

        String snapshot_id_2_1 = ""
        String snapshot_id_2_2 = ""
        // After rollback, reconnect the cluster and check data.
        connectWithDockerCluster(clusters[instance1_name]) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data in the cluster after rollback: " + res.toString())
            assertEquals(res.size(), 4)
            assertEquals(res[0]['id'], 1)
            assertEquals(res[0]['name'], '1_s_1_1')
            assertEquals(res[1]['id'], 2)
            assertEquals(res[1]['name'], '2_s_1_1')
            assertEquals(res[2]['id'], 3)
            assertEquals(res[2]['name'], '3_s_1_2')
            assertEquals(res[3]['id'], 4)
            assertEquals(res[3]['name'], '4_s_1_2')

            res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
            logger.info("snapshots in instance2: ${res}")
            assertEquals(res.size(), 3)

            sql "INSERT INTO test_table VALUES (9, '9_s_2_1')"
            sql "INSERT INTO test_table VALUES (10, '10_s_2_1')"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_2_1')"
            wait_snapshot_completed(clusters[instance1_name], "snapshot_2_1")
            snapshot_id_2_1 = get_snapshot_id("snapshot_2_1")

            sql "INSERT INTO test_table VALUES (11, '11_s_2_2')"
            sql "INSERT INTO test_table VALUES (12, '12_s_2_2')"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_2_2')"
            wait_snapshot_completed(clusters[instance1_name], "snapshot_2_2")
            snapshot_id_2_2 = get_snapshot_id("snapshot_2_2")

            res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
            logger.info("snapshots in instance2: ${res}")
            assertEquals(res.size(), 5)
            for (def snapshot : res) {
                if (snapshot['ID'] == snapshot_id_1_2) {
                    assertEquals(snapshot['COUNT'], 1)
                } else {
                    assertEquals(snapshot['COUNT'], 0)
                }
            }
        }

        // Step 3: Rollback instance2 to instance3: using snapshot_1_3
        cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id_1_3}",
            "from_instance_id": "instance2",
            "instance_id": "instance3",
            "name": "instance3_name",
            "is_successor": true
        }
        """
        logger.info("Rollback the cluster with snapshot: " + cluster_snapshot_content)
        clusters[instance1_name].rollback(cluster_snapshot_content)

        // After rollback, reconnect the cluster and check data.
        connectWithDockerCluster(clusters[instance1_name]) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data in the instance3 after rollback: " + res.toString())
            assertEquals(res.size(), 6)
            assertEquals(res[0]['id'], 1)
            assertEquals(res[0]['name'], '1_s_1_1')
            assertEquals(res[1]['id'], 2)
            assertEquals(res[1]['name'], '2_s_1_1')
            assertEquals(res[2]['id'], 3)
            assertEquals(res[2]['name'], '3_s_1_2')
            assertEquals(res[3]['id'], 4)
            assertEquals(res[3]['name'], '4_s_1_2')
            assertEquals(res[4]['id'], 5)
            assertEquals(res[4]['name'], '5_s_1_3')
            assertEquals(res[5]['id'], 6)
            assertEquals(res[5]['name'], '6_s_1_3')

            res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
            logger.info("snapshots in instance3: ${res}")
            assertEquals(res.size(), 5)
            for (def snapshot : res) {
                if (snapshot['ID'] == snapshot_id_1_2 || snapshot['ID'] == snapshot_id_1_3) {
                    assertEquals(snapshot['COUNT'], 1)
                } else {
                    assertEquals(snapshot['COUNT'], 0)
                }
            }
        }

        // Step 4: Clone instance2 to instance4: using snapshot_1_3
        cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id_1_3}",
            "from_instance_id": "instance2",
            "instance_id": "instance4",
            "name": "instance4_name",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_rollback_chain_instance4",
                "endpoint": "${getS3Endpoint()}",
                "external_endpoint": "${getS3Endpoint()}",
                "region": "${getS3Region()}",
                "provider": "${getS3Provider()}"
            }
        }
        """
        logger.info("Setup instance4 with snapshot: " + cluster_snapshot_content)
        instance4_opt.clusterSnapshot = cluster_snapshot_content
        clusters[instance4_name].init(instance4_opt, true)

        connectWithDockerCluster(clusters[instance4_name]) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data in instance4 after clone: " + res.toString())
            assertEquals(res.size(), 6)
            assertEquals(res[4]['id'], 5)
            assertEquals(res[5]['id'], 6)

            sql "INSERT INTO test_table VALUES (13, '13_i_4')"
            sql "INSERT INTO test_table VALUES (14, '14_i_4')"
            res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data in instance4 after write: " + res.toString())
            assertEquals(res.size(), 8)
            assertEquals(res[6]['id'], 13)
            assertEquals(res[7]['id'], 14)

            res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
            logger.info("snapshots in instance4: ${res}")
            assertEquals(res.size(), 0)
        }

        // Step 5: Clone instance2 to instance5: using snapshot_2_1
        cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id_2_1}",
            "from_instance_id": "instance2",
            "instance_id": "instance5",
            "name": "instance5_name",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_rollback_chain_instance4",
                "endpoint": "${getS3Endpoint()}",
                "external_endpoint": "${getS3Endpoint()}",
                "region": "${getS3Region()}",
                "provider": "${getS3Provider()}"
            }
        }
        """
        logger.info("Setup instance5 with snapshot: " + cluster_snapshot_content)
        instance5_opt.clusterSnapshot = cluster_snapshot_content
        clusters[instance5_name].init(instance5_opt, true)

        connectWithDockerCluster(clusters[instance5_name]) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data in instance5 after clone: " + res.toString())
            assertEquals(res.size(), 6)
            assertEquals(res[4]['id'], 9)
            assertEquals(res[5]['id'], 10)

            res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
            logger.info("snapshots in instance5: ${res}")
            assertEquals(res.size(), 0)
        }

        connectWithDockerCluster(clusters[instance1_name]) {
            def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
            logger.info("snapshots in instance3: ${res}")
            assertEquals(res.size(), 5)
            for (def snapshot : res) {
                if (snapshot['ID'] == snapshot_id_1_2 || snapshot['ID'] == snapshot_id_2_1) {
                    assertEquals(snapshot['COUNT'], 1)
                } else if (snapshot['ID'] == snapshot_id_1_3) {
                    assertEquals(snapshot['COUNT'], 2)
                } else {
                    assertEquals(snapshot['COUNT'], 0)
                }
            }
        }

        def ms = clusters[instance1_name].getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort

        get_instance_api.call(msHttpPort, "instance1") {
            respCode, body ->
                log.info("get instance1 resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
                assertEquals(json.result.source_snapshot_id, null)
                assertEquals(json.result.source_instance_id, null)
                assertEquals(json.result.original_instance_id, null)
                assertEquals(json.result.successor_instance_id, "instance2")
        }

        get_instance_api.call(msHttpPort, "instance2") {
            respCode, body ->
                log.info("get instance2 resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
                assertEquals(json.result.source_snapshot_id, snapshot_id_1_2)
                assertEquals(json.result.source_instance_id, "instance1")
                assertEquals(json.result.original_instance_id, "instance1")
                assertEquals(json.result.successor_instance_id, "instance3")
        }

        get_instance_api.call(msHttpPort, "instance3") {
            respCode, body ->
                log.info("get instance3 resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
                assertEquals(json.result.source_snapshot_id, snapshot_id_1_3)
                assertEquals(json.result.source_instance_id, "instance1")
                assertEquals(json.result.original_instance_id, "instance1")
                assertEquals(json.result.successor_instance_id, null)
        }

        get_instance_api.call(msHttpPort, "instance4") {
            respCode, body ->
                log.info("get instance4 resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
                assertEquals(json.result.source_snapshot_id, snapshot_id_1_3)
                assertEquals(json.result.source_instance_id, "instance1")
                assertEquals(json.result.original_instance_id, null)
                assertEquals(json.result.successor_instance_id, null)
        }

        get_instance_api.call(msHttpPort, "instance5") {
            respCode, body ->
                log.info("get instance5 resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
                assertEquals(json.result.source_snapshot_id, snapshot_id_2_1)
                assertEquals(json.result.source_instance_id, "instance2")
                assertEquals(json.result.original_instance_id, null)
                assertEquals(json.result.successor_instance_id, null)
        }
    }
}


