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

import groovy.json.JsonOutput
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.suite.SuiteCluster
import org.awaitility.Awaitility

suite("test_clone_chain_compactor", "snapshot,docker") {
    // ATTN: This test only runs in cloud mode.
    if (!isCloudMode()) {
        logger.info("Skip test_clone_chain_compactor because not in cloud mode")
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

    def drop_cluster_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/drop_cluster?token=greedisgood9999"
            body request_body
            check check_func
        }
    }

    def drop_instance_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/drop_instance?token=greedisgood9999"
            body request_body
            check check_func
        }
    }

    def drop_instance = { msHttpPort, instance_id ->
        // drop be cluster
        def clusterMap = [cluster_name: "compute_cluster", cluster_id: "compute_cluster_id"]
        def instance = [instance_id: "${instance_id}", cluster: clusterMap]
        def jsonOutput = new JsonOutput()
        def dropClusterBody  = jsonOutput.toJson(instance)
        drop_cluster_api.call(msHttpPort, dropClusterBody) {
            respCode, body ->
                log.info("drop be cluster http cli result: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // drop fe cluster
        clusterMap = [cluster_name: "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER", cluster_id: "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        dropClusterBody  = jsonOutput.toJson(instance)
        drop_cluster_api.call(msHttpPort, dropClusterBody) {
            respCode, body ->
                log.info("drop fe cluster http cli result: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // drop instance
        instance = [instance_id: "${instance_id}"]
        def dropInstanceBody = jsonOutput.toJson(instance)
        drop_instance_api.call(msHttpPort, dropInstanceBody) {
            respCode, body ->
                log.info("drop instance http cli result: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // get instance
        get_instance_api.call(msHttpPort, "${instance_id}") {
            respCode, body ->
                log.info("get instance resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
                assertTrue(json.result.status.equalsIgnoreCase("DELETED"))
        }
    }

    def cluster_prefix = "regression_test_clone_chain_compactor_"
    def cluster_1 = cluster_prefix + "cluster_1"
    def cluster_2 = cluster_prefix + "cluster_2"
    def cluster_3 = cluster_prefix + "cluster_3"

    def cluster_1_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "cluster_1_instance_id",
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
            "enable_check_fe_drop_in_safe_time=false",
        ],
        recycleConfigs: [
            "recycle_interval_seconds=1",
            "recycler_sleep_before_scheduling_seconds=1",
            "enable_snapshot_data_migrator=true",
            "enable_snapshot_chain_compactor=true",
        ])

    def cluster_2_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 0,
        instanceId: "cluster_2_instance_id",
        externalMsCluster: cluster_1,
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

    def cluster_3_opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 0,
        instanceId: "cluster_3_instance_id",
        externalMsCluster: cluster_1,
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

    def manual_init_clusters = [cluster_2, cluster_3].toSet()
    def cluster1_snapshot_id = ""
    dockers(["${cluster_1}": cluster_1_opt, "${cluster_2}": cluster_2_opt, "${cluster_3}": cluster_3_opt], manual_init_clusters) { clusters ->
        // Step 1: Create a snapshot in the base cluster
        String snapshot_id = ""
        connectWithDockerCluster(clusters[cluster_1]) {
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
            for (int i = 0; i < 10; i++) {
                sql "INSERT INTO test_table VALUES (${i}, 'cluster1_data_${i}')"
            }

            // empty table
            sql """
                CREATE TABLE IF NOT EXISTS test_empty_table (
                    id INT,
                    name VARCHAR(100)
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """

            // mow table
            sql """
                CREATE TABLE IF NOT EXISTS test_mow_table (
                    id INT,
                    name VARCHAR(100)
                ) UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_mow_table VALUES (1, 'cluster0_data')"
            sql "INSERT INTO test_mow_table VALUES (1, 'cluster1_data_1')"
            sql "INSERT INTO test_mow_table VALUES (2, 'cluster1_data_2')"

            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_label')"
            wait_snapshot_completed(clusters[cluster_1], "snapshot_label")
            snapshot_id = get_snapshot_id("snapshot_label")
            cluster1_snapshot_id = snapshot_id
        }

        // Step 2: Restore the snapshot in the derived cluster
        def cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id}",
            "from_instance_id": "cluster_1_instance_id",
            "instance_id": "cluster_2_instance_id",
            "name": "cluster_2_instance",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_clone_chain_compactor_2",
                "endpoint": "${getS3Endpoint()}",
                "external_endpoint": "${getS3Endpoint()}",
                "region": "${getS3Region()}",
                "provider": "${getS3Provider()}"
            }
        }
        """
        logger.info("Setup derived cluster with snapshot: " + cluster_snapshot_content)
        cluster_2_opt.clusterSnapshot = cluster_snapshot_content
        clusters[cluster_2].init(cluster_2_opt, true)

        connectWithDockerCluster(clusters[cluster_2]) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data in derived cluster after clone: " + res.toString())
            assertEquals(res.size(), 10)

            res = sql_return_maparray "SELECT * FROM test_empty_table ORDER BY id"
            logger.info("test_empty_table in derived cluster after clone: " + res.toString())
            assertEquals(res.size(), 0)

            res = sql_return_maparray "SELECT * FROM test_mow_table ORDER BY id"
            logger.info("test_mow_table in derived cluster after clone: " + res.toString())
            assertEquals(res.size(), 2)
            assertEquals(res[0]['id'], 1)
            assertEquals(res[0]['name'], 'cluster1_data_1')
            assertEquals(res[1]['id'], 2)
            assertEquals(res[1]['name'], 'cluster1_data_2')

            // Below inserts are not included in the base cluster
            sql "INSERT INTO test_table VALUES (2, 'cluster2_data')"
            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'snapshot_label')"
            wait_snapshot_completed(clusters[cluster_2], "snapshot_label")
            snapshot_id = get_snapshot_id("snapshot_label")

            sql "INSERT INTO test_table VALUES (3, 'cluster2_new_data')"
        }

        // Step 3: Restore the snapshot in the second derived cluster
        cluster_snapshot_content = """
        {
            "from_snapshot_id": "${snapshot_id}",
            "from_instance_id": "cluster_2_instance_id",
            "instance_id": "cluster_3_instance_id",
            "name": "cluster_3_instance",
            "is_read_only": false,
            "obj_info": {
                "ak": "${getS3AK()}",
                "sk": "${getS3SK()}",
                "bucket": "${getS3BucketName()}",
                "prefix": "regression_test_clone_chain_compactor_3",
                "endpoint": "${getS3Endpoint()}",
                "external_endpoint": "${getS3Endpoint()}",
                "region": "${getS3Region()}",
                "provider": "${getS3Provider()}"
            }
        }
        """
        logger.info("Setup derived cluster with snapshot: " + cluster_snapshot_content)
        cluster_3_opt.clusterSnapshot = cluster_snapshot_content
        clusters[cluster_3].init(cluster_3_opt, true)

        connectWithDockerCluster(clusters[cluster_3]) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data in derived cluster after clone: " + res.toString())
            assertEquals(res.size(), 11)

            res = sql_return_maparray "SELECT * FROM test_empty_table ORDER BY id"
            logger.info("test_empty_table in derived cluster after clone: " + res.toString())
            assertEquals(res.size(), 0)

            res = sql_return_maparray "SELECT * FROM test_mow_table ORDER BY id"
            logger.info("test_mow_table in derived cluster after clone: " + res.toString())
            assertEquals(res.size(), 2)
            assertEquals(res[0]['id'], 1)
            assertEquals(res[0]['name'], 'cluster1_data_1')
            assertEquals(res[1]['id'], 2)
            assertEquals(res[1]['name'], 'cluster1_data_2')
        }

        // drop snapshot in instance1
        connectWithDockerCluster(clusters[cluster_1]) {
            for (int i = 1; i <= 20; i++) {
                try {
                    sql "ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '${cluster1_snapshot_id}'"
                    def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
                    logger.info("Cluster snapshots after dropping: " + res.toString())
                    break
                } catch (Exception e) {
                    assertTrue(e.getMessage().contains("cannot drop snapshot that is referenced by other instance"))
                    def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
                    logger.info("Cluster snapshots after dropping " + i + " times: " + res.toString())
                    sleep(5000)
                }
            }
            def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots"
            logger.info("Cluster snapshots after dropping: " + res.toString())
            assertEquals(res.size(), 0)
        }

        // get instance2
        def ms = clusters[cluster_1].getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        get_instance_api(msHttpPort, "cluster_2_instance_id") {
            respCode, body ->
                log.info("get instance2 resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
                assertEquals(json.result.source_instance_id, "cluster_1_instance_id")
                assertEquals(json.result.source_snapshot_id, null)
        }

        // check data in instance2
        connectWithDockerCluster(clusters[cluster_2]) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data in derived cluster after clone: " + res.toString())
            assertEquals(res.size(), 12)

            res = sql_return_maparray "SELECT * FROM test_empty_table ORDER BY id"
            logger.info("test_empty_table in derived cluster after clone: " + res.toString())
            assertEquals(res.size(), 0)

            res = sql_return_maparray "SELECT * FROM test_mow_table ORDER BY id"
            logger.info("test_mow_table in derived cluster after clone: " + res.toString())
            assertEquals(res.size(), 2)
            assertEquals(res[0]['id'], 1)
            assertEquals(res[0]['name'], 'cluster1_data_1')
            assertEquals(res[1]['id'], 2)
            assertEquals(res[1]['name'], 'cluster1_data_2')
        }

        // drop instance1
        drop_instance(msHttpPort, "cluster_1_instance_id")

        // check instance1 is recycled
        def recycled = false
        for (int i = 1; i <= 20; i++) {
            get_instance_api(msHttpPort, "cluster_1_instance_id") {
                respCode, body ->
                    log.info("get instance1 resp: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    if (json.code.equalsIgnoreCase("INTERNAL_ERROR")) {
                        if (json.msg.contains("KeyNotFound")) {
                            recycled = true
                        }
                    }
            }
            if (recycled) {
                break
            } else {
                sleep(5000)
            }
        }
        assertTrue(recycled)

        // check data in instance2
        connectWithDockerCluster(clusters[cluster_2]) {
            sql "USE test_db"
            def res = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data in derived cluster after clone: " + res.toString())
            assertEquals(res.size(), 12)

            res = sql_return_maparray "SELECT * FROM test_empty_table ORDER BY id"
            logger.info("test_empty_table in derived cluster after clone: " + res.toString())
            assertEquals(res.size(), 0)

            res = sql_return_maparray "SELECT * FROM test_mow_table ORDER BY id"
            logger.info("test_mow_table in derived cluster after clone: " + res.toString())
            assertEquals(res.size(), 2)
            assertEquals(res[0]['id'], 1)
            assertEquals(res[0]['name'], 'cluster1_data_1')
            assertEquals(res[1]['id'], 2)
            assertEquals(res[1]['name'], 'cluster1_data_2')
        }
    }
}


