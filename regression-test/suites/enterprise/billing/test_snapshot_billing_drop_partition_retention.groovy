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

suite("test_snapshot_billing_drop_partition_retention", "snapshot,billing,docker") {
    if (!isCloudMode()) {
        logger.info("Skip test_snapshot_billing_drop_partition_retention because not in cloud mode")
        return
    }

    def token = "greedisgood9999"
    def instance_id = "default_instance_id"
    def jsonOutput = new JsonOutput()

    def wait_snapshot_completed = { snapshot_label ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(1)).atMost(java.time.Duration.ofMinutes(5)).until {
            def res = sql_return_maparray("SELECT * FROM information_schema.cluster_snapshots WHERE LABEL='${snapshot_label}'")
            logger.info("Snapshot ${snapshot_label} status: " + res.toString())
            return res.size() == 1 && res[0]['STATE'] == 'SNAPSHOT_NORMAL'
        }
    }

    def get_snapshot_id = { snapshot_label ->
        def res = sql_return_maparray("SELECT * FROM information_schema.cluster_snapshots WHERE LABEL='${snapshot_label}' ORDER BY CREATE_AT DESC")
        assertEquals(res.size(), 1)
        return res[0]['ID']
    }

    def parse_data_size_to_bytes = { size_text ->
        assertNotNull(size_text, "SHOW DATA size must not be null")
        def matcher = (size_text as String).trim() =~ /^([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)$/
        assertTrue(matcher.matches(), "Unexpected SHOW DATA size format: ${size_text}")
        def value = new BigDecimal(matcher[0][1])
        def unit = matcher[0][2].toUpperCase()
        def multiplier = switch (unit) {
            case 'B' -> 1L
            case 'KB' -> 1024L
            case 'MB' -> 1024L * 1024L
            case 'GB' -> 1024L * 1024L * 1024L
            case 'TB' -> 1024L * 1024L * 1024L * 1024L
            case 'PB' -> 1024L * 1024L * 1024L * 1024L * 1024L
            default -> {
                assertTrue(false, "Unsupported SHOW DATA unit: ${unit}")
                yield 1L
            }
        }
        return value.multiply(BigDecimal.valueOf(multiplier)).longValue()
    }

    def get_table_data_size = {
        def show_data = sql_return_maparray("SHOW DATA")
        logger.info("SHOW DATA result: " + show_data.toString())
        def table_row = show_data.find { it['TableName'] == 'snapshot_billing_drop_partition_lineitem' }
        assertNotNull(table_row, "snapshot_billing_drop_partition_lineitem must appear in SHOW DATA")
        return parse_data_size_to_bytes(table_row['Size'])
    }

    def get_instance_api = { ms_http_port, target_instance_id, check_func ->
        httpTest {
            op "get"
            endpoint ms_http_port
            uri "/MetaService/http/get_instance?token=${token}&instance_id=${target_instance_id}"
            check check_func
        }
    }

    def list_snapshot_api = { ms_http_port, request_body, check_func ->
        httpTest {
            endpoint ms_http_port
            uri "/MetaService/http/list_snapshot?token=${token}"
            body request_body
            check check_func
        }
    }

    def recycle_instance_api = { recycler_http_port, request_body, check_func ->
        httpTest {
            endpoint recycler_http_port
            uri "/RecyclerService/http/recycle_instance?token=${token}"
            body request_body
            check check_func
        }
    }

    def recycle_job_info_api = { recycler_http_port, target_instance_id, check_func ->
        httpTest {
            endpoint recycler_http_port
            uri "/RecyclerService/http/recycle_job_info?token=${token}&instance_id=${target_instance_id}"
            op "get"
            check check_func
        }
    }

    def get_snapshot_billing = { ms_http_port, target_instance_id, snapshot_id ->
        def request_body = jsonOutput.toJson([
            instance_id: target_instance_id,
            required_snapshot_id: snapshot_id,
        ])
        def snapshot_billing = null
        list_snapshot_api.call(ms_http_port, request_body) { respCode, body ->
            logger.info("list_snapshot resp: ${body} ${respCode}".toString())
            assertEquals(respCode, 200)
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"), "list_snapshot should succeed, body=${body}")
            assertNotNull(json.result?.snapshots, "list_snapshot must return snapshots, body=${body}")
            assertEquals(json.result.snapshots.size(), 1)
            def snapshot = json.result.snapshots[0]
            snapshot_billing = [
                snapshot_id: snapshot.snapshot_id,
                snapshot_meta_image_size: snapshot.snapshot_meta_image_size == null ? -1L : Long.parseLong(snapshot.snapshot_meta_image_size.toString()),
                snapshot_logical_data_size: snapshot.snapshot_logical_data_size == null ? -1L : Long.parseLong(snapshot.snapshot_logical_data_size.toString()),
                snapshot_retained_data_size: snapshot.snapshot_retained_data_size == null ? -1L : Long.parseLong(snapshot.snapshot_retained_data_size.toString()),
                snapshot_billable_data_size: snapshot.snapshot_billable_data_size == null ? -1L : Long.parseLong(snapshot.snapshot_billable_data_size.toString()),
            ]
        }
        return snapshot_billing
    }

    def get_instance_billing = { ms_http_port, target_instance_id ->
        def instance_billing = null
        get_instance_api.call(ms_http_port, target_instance_id) { respCode, body ->
            logger.info("get_instance resp: ${body} ${respCode}".toString())
            assertEquals(respCode, 200)
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"), "get_instance should succeed, body=${body}")
            instance_billing = [
                left_data_size: json.result.snapshot_retained_data_size == null ? 0L : Long.parseLong(json.result.snapshot_retained_data_size.toString()),
                snapshot_billable_data_size: json.result.snapshot_billable_data_size == null ? 0L : Long.parseLong(json.result.snapshot_billable_data_size.toString()),
            ]
        }
        return instance_billing
    }

    def get_recycle_job_info = { recycler_http_port, target_instance_id ->
        def recycle_job_info = null
        recycle_job_info_api.call(recycler_http_port, target_instance_id) { respCode, body ->
            logger.info("recycle_job_info resp: ${body} ${respCode}".toString())
            assertEquals(respCode, 200)
            def json = parseJson(body.trim())
            recycle_job_info = [
                last_finish_time_ms: json.last_finish_time_ms == null ? -1L : Long.parseLong(json.last_finish_time_ms.toString()),
                last_success_time_ms: json.last_success_time_ms == null ? -1L : Long.parseLong(json.last_success_time_ms.toString()),
            ]
        }
        return recycle_job_info
    }

    def trigger_recycle_and_wait = { recycler_http_port, target_instance_id ->
        def before_job_info = get_recycle_job_info(recycler_http_port, target_instance_id)
        def request_body = jsonOutput.toJson([instance_ids: [target_instance_id]])
        recycle_instance_api.call(recycler_http_port, request_body) { respCode, body ->
            logger.info("recycle_instance resp: ${body} ${respCode}".toString())
            assertEquals(respCode, 200)
            assertTrue(body.trim().equalsIgnoreCase("OK"), "recycle_instance should return OK, body=${body}")
        }

        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(2)).atMost(java.time.Duration.ofMinutes(2)).until {
            def after_job_info = get_recycle_job_info(recycler_http_port, target_instance_id)
            logger.info("recycle job info before=${before_job_info}, after=${after_job_info}".toString())
            return after_job_info.last_finish_time_ms > before_job_info.last_finish_time_ms &&
                    after_job_info.last_finish_time_ms == after_job_info.last_success_time_ms
        }
    }

    def opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1, recyclerNum: 1,
        instanceId: instance_id,
        feConfigs: [
            "cloud_auto_snapshot_min_interval_seconds=5",
            "tablet_stat_update_interval_second=1",
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
        ])

    docker(opt) { ->
        sql "CREATE DATABASE IF NOT EXISTS regression_test_enterprise_billing"
        sql "USE regression_test_enterprise_billing"
        sql "DROP TABLE IF EXISTS snapshot_billing_drop_partition_lineitem FORCE"
        sql """
            CREATE TABLE IF NOT EXISTS snapshot_billing_drop_partition_lineitem (
                id BIGINT NOT NULL,
                l_shipdate DATE NOT NULL,
                payload STRING NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            PARTITION BY RANGE(l_shipdate)
            (
                PARTITION p000000 VALUES [("0000-01-01"), ("1992-01-01")),
                PARTITION p199201 VALUES [("1992-01-01"), ("1992-02-01")),
                PARTITION p199202 VALUES [("1992-02-01"), ("1992-03-01")),
                PARTITION p199203 VALUES [("1992-03-01"), ("1992-04-01"))
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """

        for (int round = 0; round < 24; round++) {
            sql """
                INSERT INTO snapshot_billing_drop_partition_lineitem VALUES
                    (${round * 4 + 1}, '1992-01-05', concat(repeat('drop_partition_payload_a_', 512), '${round}')),
                    (${round * 4 + 2}, '1992-01-15', concat(repeat('drop_partition_payload_b_', 512), '${round}')),
                    (${round * 4 + 3}, '1992-02-05', concat(repeat('keep_partition_payload_c_', 512), '${round}')),
                    (${round * 4 + 4}, '1992-03-05', concat(repeat('keep_partition_payload_d_', 512), '${round}'))
            """
        }

        def data_size_before_snapshot = 0L
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(2)).atMost(java.time.Duration.ofMinutes(2)).until {
            data_size_before_snapshot = get_table_data_size()
            return data_size_before_snapshot > 0
        }

        sql "ADMIN SET CLUSTER SNAPSHOT FEATURE ON"
        sql "ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '86400', 'label' = 'snapshot_billing_drop_partition_repro')"
        wait_snapshot_completed("snapshot_billing_drop_partition_repro")

        def snapshot_id = get_snapshot_id("snapshot_billing_drop_partition_repro")
        def msList = cluster.getMetaservices()
        def (msIp, msPort) = msList[0].getHttpAddress()
        def ms_http_port = "${msIp}:${msPort}"
        def recyclerList = cluster.getAllRecyclers()
        def (recyclerIp, recyclerPort) = recyclerList[0].getHttpAddress()
        def recycler_http_port = "${recyclerIp}:${recyclerPort}"

        trigger_recycle_and_wait(recycler_http_port, instance_id)
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(2)).atMost(java.time.Duration.ofMinutes(2)).until {
            def snapshot_billing = get_snapshot_billing(ms_http_port, instance_id, snapshot_id)
            return snapshot_billing.snapshot_retained_data_size >= 0 && snapshot_billing.snapshot_billable_data_size >= 0
        }

        def baseline_snapshot_billing = get_snapshot_billing(ms_http_port, instance_id, snapshot_id)
        def baseline_instance_billing = get_instance_billing(ms_http_port, instance_id)
        logger.info("baseline snapshot billing: ${baseline_snapshot_billing}".toString())
        logger.info("baseline instance billing: ${baseline_instance_billing}".toString())

        assertEquals(baseline_snapshot_billing.snapshot_retained_data_size, 0L)
        assertEquals(baseline_instance_billing.left_data_size, 0L)
        assertTrue(baseline_snapshot_billing.snapshot_logical_data_size > 0,
            "snapshot_logical_data_size must be positive after snapshot creation")
        assertTrue(baseline_snapshot_billing.snapshot_billable_data_size >= baseline_snapshot_billing.snapshot_meta_image_size,
            "baseline snapshot billable size must cover snapshot meta image size")

        sql """ ALTER TABLE snapshot_billing_drop_partition_lineitem DROP PARTITION p199201 FORCE """

        def dropped_partition_rows = sql """
            SELECT COUNT(*)
            FROM snapshot_billing_drop_partition_lineitem
            WHERE l_shipdate >= '1992-01-01' AND l_shipdate < '1992-02-01'
        """
        assertEquals(dropped_partition_rows[0][0], 0)

        def data_size_after_drop = data_size_before_snapshot
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(2)).atMost(java.time.Duration.ofMinutes(2)).until {
            data_size_after_drop = get_table_data_size()
            return data_size_after_drop < data_size_before_snapshot
        }

        def final_snapshot_billing = baseline_snapshot_billing
        def final_instance_billing = baseline_instance_billing
        def retained_condition_met = false
        for (int round = 1; round <= 20; round++) {
            trigger_recycle_and_wait(recycler_http_port, instance_id)
            final_snapshot_billing = get_snapshot_billing(ms_http_port, instance_id, snapshot_id)
            final_instance_billing = get_instance_billing(ms_http_port, instance_id)
            logger.info("round=${round}, snapshot billing=${final_snapshot_billing}, instance billing=${final_instance_billing}".toString())

            if (final_snapshot_billing.snapshot_retained_data_size > baseline_snapshot_billing.snapshot_retained_data_size &&
                    final_snapshot_billing.snapshot_billable_data_size > baseline_snapshot_billing.snapshot_billable_data_size &&
                    final_snapshot_billing.snapshot_billable_data_size > final_snapshot_billing.snapshot_meta_image_size) {
                retained_condition_met = true
                break
            }
        }

        assertTrue(retained_condition_met,
            "Snapshot ${snapshot_id} did not satisfy condition 'drop partition should retain removed partition data' after 20 recycler rounds; baseline_snapshot=${baseline_snapshot_billing}, baseline_instance=${baseline_instance_billing}, final_snapshot=${final_snapshot_billing}, final_instance=${final_instance_billing}, data_size_before_snapshot=${data_size_before_snapshot}, data_size_after_drop=${data_size_after_drop}")

        assertEquals(final_snapshot_billing.snapshot_logical_data_size, baseline_snapshot_billing.snapshot_logical_data_size)
        assertTrue(final_snapshot_billing.snapshot_retained_data_size + final_instance_billing.left_data_size > 0,
            "snapshot_retained_data_size + left_data_size must be positive after drop partition")
        assertTrue(final_snapshot_billing.snapshot_billable_data_size > final_snapshot_billing.snapshot_meta_image_size,
            "snapshot_billable_data_size must exceed snapshot_meta_image_size after retained data is generated")
    }
}
