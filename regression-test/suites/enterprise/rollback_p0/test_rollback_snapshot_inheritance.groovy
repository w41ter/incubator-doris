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
import org.awaitility.Awaitility

suite("test_rollback_snapshot_inheritance", "snapshot,docker") {
    if (!isCloudMode()) {
        logger.info("Skip test_rollback_snapshot_inheritance because not in cloud mode")
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

    def do_recycle_func = { instance_id, recyclerHttpPort ->
        def triggerRecycleBody = [instance_ids: ["${instance_id}"]]
        def jsonOutput = new JsonOutput()
        def triggerRecycleJson = jsonOutput.toJson(triggerRecycleBody)
        httpTest {
            endpoint recyclerHttpPort
            body triggerRecycleJson
            uri "/RecyclerService/http/recycle_instance?token=greedisgood9999"
        }
    }

    def get_instance_api = { msHttpPort, instance_id, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/get_instance?token=greedisgood9999&instance_id=${instance_id}"
            check check_func
        }
    }

    def compact_snapshot = { msHttpPort, instance_id ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/compact_snapshot?token=greedisgood9999&instance_id=${instance_id}"
            check { respCode, body ->
                log.info("compact_snapshot resp for ${instance_id}: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"),
                    "compact_snapshot failed for instance_id=${instance_id}: ${body}")
            }
        }
    }

    def wait_compact_done = { msHttpPort, instance_id ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(2)).atMost(java.time.Duration.ofMinutes(5)).until {
            def done = false
            get_instance_api.call(msHttpPort, instance_id) { respCode, body ->
                log.info("wait_compact_done get_instance resp for ${instance_id}: ${body}".toString())
                def json = parseJson(body)
                if (json.code.equalsIgnoreCase("OK") && json.result != null) {
                    done = (json.result.snapshot_compact_status == "SNAPSHOT_COMPACT_DONE")
                }
            }
            return done
        }
    }

    def decouple_instance_api = { msHttpPort, instance_id ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/decouple_instance?token=greedisgood9999&instance_id=${instance_id}"
            check { respCode, body ->
                log.info("decouple_instance resp for ${instance_id}: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"),
                    "decouple_instance failed for instance_id=${instance_id}: ${body}")
            }
        }
    }

    def set_snapshot_property = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/set_snapshot_property?token=greedisgood9999"
            body request_body
            check check_func
        }
    }

    def get_snapshot_id = { snapshot_label ->
        def res = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots WHERE LABEL='${snapshot_label}'"
        assertEquals(res.size(), 1)
        return res[0]['ID']
    }

    def get_all_visible_snapshots = {
        return sql_return_maparray("SELECT * FROM information_schema.cluster_snapshots")
    }

    def get_normal_auto_snapshots = {
        def res = get_all_visible_snapshots()
        return res.findAll { it['AUTO'] == true && it['STATE'] == 'SNAPSHOT_NORMAL' }
    }

    def wait_auto_snapshot_at_least = { cluster, min_count ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(2)).atMost(java.time.Duration.ofMinutes(5)).until {
            connectWithDockerCluster(cluster) {
                def autoSnapshots = get_normal_auto_snapshots()
                logger.info("Current normal auto snapshots: " + autoSnapshots.toString())
                return autoSnapshots.size() >= min_count
            }
        }
    }

    def wait_snapshots_absent = { cluster, instance_id, snapshot_ids ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(3)).atMost(java.time.Duration.ofMinutes(5)).until {
            // do_recycle_func(instance_id, recyclerHttpPort)
            connectWithDockerCluster(cluster) {
                def allSnapshots = get_all_visible_snapshots()
                def visibleIds = allSnapshots.collect { it['ID'] } as Set
                logger.info("Visible snapshots while waiting for recycle on ${instance_id}: " + allSnapshots.toString())
                return snapshot_ids.every { !visibleIds.contains(it) }
            }
        }
    }

    def old_instance_id = "rollback_snapshot_old_instance"
    def new_instance_id = "rollback_snapshot_new_instance"
    def opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1, recyclerNum: 1,
        instanceId: old_instance_id,
        feConfigs: [
            "cloud_auto_snapshot_min_interval_seconds=5",
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
            "snapshot_min_interval_seconds=5",
        ],
        recycleConfigs: [
            "recycle_interval_seconds=1",
            "recycler_sleep_before_scheduling_seconds=1",
            "enable_snapshot_data_migrator=true",
            "enable_snapshot_chain_compactor=true",
        ])

    docker(opt) {
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        def recyclerService = cluster.getAllRecyclers()
        def (ip, port) = recyclerService[0].getHttpAddress()
        def recyclerHttpPort = "${ip}:${port}"

        String rollback_snapshot_id = ""
        String new_instance_snapshot_id = ""
        def inheritedAutoSnapshotIds = [] as Set

        connectWithDockerCluster(cluster) {
            sql "CREATE DATABASE IF NOT EXISTS test_db"
            sql "USE test_db"
            sql """
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT,
                    name VARCHAR(100)
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO test_table VALUES (1, 'base_row_1')"
            sql "INSERT INTO test_table VALUES (2, 'base_row_2')"

            sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN SET AUTO CLUSTER SNAPSHOT PROPERTIES('snapshot_interval_seconds'='5', 'max_reserved_snapshots'='35')"
        }

        wait_auto_snapshot_at_least(cluster, 1)

        connectWithDockerCluster(cluster) {
            def autoSnapshots = get_normal_auto_snapshots()
            inheritedAutoSnapshotIds.addAll(autoSnapshots.collect { it['ID'] })
            assertTrue(inheritedAutoSnapshotIds.size() >= 1)

            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'rollback_anchor_snapshot')"
            wait_snapshot_completed(cluster, "rollback_anchor_snapshot")
            rollback_snapshot_id = get_snapshot_id("rollback_anchor_snapshot")

            def allSnapshots = get_all_visible_snapshots()
            logger.info("Snapshots on old instance before rollback: " + allSnapshots.toString())
            assertTrue(allSnapshots.any { it['ID'] == rollback_snapshot_id && it['AUTO'] == false })
        }

        def rollbackRequest = """
        {
            "from_snapshot_id": "${rollback_snapshot_id}",
            "from_instance_id": "${old_instance_id}",
            "instance_id": "${new_instance_id}",
            "name": "rollback_snapshot_new_instance_name",
            "is_successor": true
        }
        """
        logger.info("Rollback the cluster with snapshot: " + rollbackRequest)
        cluster.rollback(rollbackRequest )

        connectWithDockerCluster(cluster) {
            sql "USE test_db"
            def data = sql_return_maparray "SELECT * FROM test_table ORDER BY id"
            logger.info("Data after rollback: " + data.toString())
            assertEquals(data.size(), 2)

            def allSnapshots = get_all_visible_snapshots()
            logger.info("Snapshots visible on new instance immediately after rollback: " + allSnapshots.toString())
            def autoSnapshots = allSnapshots.findAll { it['AUTO'] == true && it['STATE'] == 'SNAPSHOT_NORMAL' }
            def autoSnapshotIds = autoSnapshots.collect { it['ID'] } as Set

            assertTrue(autoSnapshotIds.containsAll(inheritedAutoSnapshotIds))
            assertTrue(allSnapshots.any { it['ID'] == rollback_snapshot_id && it['AUTO'] == false })
        }

        compact_snapshot(msHttpPort, new_instance_id)
        wait_compact_done(msHttpPort, new_instance_id)
        decouple_instance_api(msHttpPort, new_instance_id)

        get_instance_api.call(msHttpPort, new_instance_id) { respCode, body ->
            log.info("get_instance after decouple resp: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"))
            assertEquals(json.result.source_instance_id, null)
            assertEquals(json.result.source_snapshot_id, null)
        }

        connectWithDockerCluster(cluster) {
            sql "USE test_db"
            // sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
            sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'new_instance_snapshot')"
            wait_snapshot_completed(cluster, "new_instance_snapshot")
            new_instance_snapshot_id = get_snapshot_id("new_instance_snapshot")

            // The manual snapshot created after decouple should be recyclable.
            sql "ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '${rollback_snapshot_id}'"

            def snapshotProperties = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshot_properties"
            logger.info("Snapshot properties on new instance: " + snapshotProperties.toString())
            assertEquals(snapshotProperties.size(), 1)
            assertEquals(snapshotProperties[0]['SNAPSHOT_ENABLED'], 'YES')
        }

        def property_json = ["max_reserved_snapshots": "1"]
        def instance_property_json = ["instance_id": new_instance_id, "properties": property_json]
        def jsonOutput = new JsonOutput()
        def property_body = jsonOutput.toJson(instance_property_json)
        set_snapshot_property.call(msHttpPort, property_body) { respCode, body ->
            log.info("set_snapshot_property for ${new_instance_id}: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        wait_snapshots_absent(cluster, old_instance_id, [rollback_snapshot_id])
        wait_snapshots_absent(cluster, old_instance_id, inheritedAutoSnapshotIds as List)

        connectWithDockerCluster(cluster) {
            def allSnapshots = get_all_visible_snapshots()
            logger.info("Final snapshots visible on new instance after recycle: " + allSnapshots.toString())
            def visibleIds = allSnapshots.collect { it['ID'] } as Set

            assertFalse(visibleIds.contains(rollback_snapshot_id))
            inheritedAutoSnapshotIds.each { inheritedId ->
                assertFalse(visibleIds.contains(inheritedId))
            }
            assertTrue(visibleIds.contains(new_instance_snapshot_id))
        }
    }
}