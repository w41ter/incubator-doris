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

suite("test_switch_multi_version_status", "snapshot,docker") {
    if (!isCloudMode()) {
        logger.info("Skip test_switch_multi_version_status because not in cloud mode")
        return
    }

    def create_unique_mow_table = { tableName ->
        sql """ DROP TABLE IF EXISTS ${tableName} """

        sql """
            CREATE TABLE ${tableName} (
                k1 INT,
                k2 INT,
                v1 INT
            ) ENGINE=OLAP
            UNIQUE KEY(k1, k2)
            AUTO PARTITION BY LIST(k1) ()
            DISTRIBUTED BY HASH(k2) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2",
                "enable_unique_key_merge_on_write" = "true"
            )
        """
    }

    // Insert data into the table, for k1 in partitions, insert rows from start to end
    def insert_data = { tableName, start, end, partitions ->
        for (i in start..end) {
            def values = []
            for (p in partitions) {
                values.add("(${p}, ${i}, ${i})")
            }
            sql """ INSERT INTO ${tableName} VALUES ${values.join(",")} """
        }
    }

    def instance_id = "instance_id"
    def token = "greedisgood9999"

    def get_multi_version_status = { host ->
        def url = "http://${host}/MetaService/http/get_instance?token=${token}&instance_id=${instance_id}"
        def (code, out, err) = curl('GET', url)
        assert code == 0 : "Failed to get multi version status: ${out} ${err}"
        def json = parseJson(out)
        assert json["code"] == "OK" : "Get instance failed: ${out} ${err}"
        if (json["result"].containsKey("multi_version_status")) {
            return json["result"]["multi_version_status"]
        }
        return "MULTI_VERSION_DISABLED"
    }

    def set_multi_version_status = { host, status ->
        def url = "http://${host}/MetaService/http/set_multi_version_status?token=${token}&instance_id=${instance_id}&multi_version_status=${status}"
        def (code, out, err) = curl('POST', url)
        assert code == 0 : "Failed to set multi version status: ${out} ${err}"
        def json = parseJson(out)
        assert json["code"] == "OK" : "Set multi version status failed: ${out} ${err}"
    }

    def get_snapshot_switch_status = { host ->
        def url = "http://${host}/MetaService/http/get_snapshot_property?token=${token}&instance_id=${instance_id}"
        def (code, out, err) = curl('GET', url)
        assert code == 0 : "Failed to get snapshot switch status: ${out} ${err}"
        def json = parseJson(out)
        assert json["code"] == "OK" : "Get snapshot property failed: ${out} ${err}"
        return json["result"]["status"]
    }

    def wait_snapshot_switch_status = { host, expected_status ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(2)).atMost(java.time.Duration.ofMinutes(5)).until {
            def status = get_snapshot_switch_status(host)
            return status == expected_status
        }
    }

    def wait_alter_table_column_finished = { tableName ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(2)).atMost(java.time.Duration.ofMinutes(5)).until {
            def result = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' AND State!='FINISHED'"
            return result.size() == 0 ? true : false
        }
    }

    def cluster_name = "regression_test_switch_multi_version_status"
    def opt = new ClusterOptions(
        cloudMode: true, feNum: 1, beNum: 1, msNum: 1,
        instanceId: "${instance_id}",
        beConfigs: [
            "delete_bitmap_store_write_version=3",
            "delete_bitmap_store_read_version=3",
        ],
        msConfigs: [
            "enable_split_rowset_meta=true",
            "enable_split_tablet_schema_pb=true",
            "enable_multi_version_status=false",
            "multi_version_status_check_interval_seconds=1",
            "log_verbose_modules=*",
            "log_immediate_flush=true",
        ],
        recycleConfigs: [
            "recycle_interval_seconds=1",
            "recycler_sleep_before_scheduling_seconds=1",
            "enable_snapshot_data_migrator=true",
            "log_verbose_modules=*",
            "log_immediate_flush=true",
        ])

    docker(opt) {
        def tableName = "mow_table"
        def msList = cluster.getMetaservices()
        def (ip, port) = msList[0].getHttpAddress()
        def host = "${ip}:${port}"
        create_unique_mow_table(tableName)
        insert_data(tableName, 1, 10, [1,2])

        assertEquals("MULTI_VERSION_DISABLED", get_multi_version_status(host))

        // DISABLED -> DISABLED
        set_multi_version_status(host, "MULTI_VERSION_DISABLED")

        // DISABLED -> WRITE_ONLY -> DISABLED
        set_multi_version_status(host, "MULTI_VERSION_WRITE_ONLY")
        set_multi_version_status(host, "MULTI_VERSION_WRITE_ONLY")
        assertEquals("MULTI_VERSION_WRITE_ONLY", get_multi_version_status(host))
        insert_data(tableName, 11, 20, [2,3])
        set_multi_version_status(host, "MULTI_VERSION_DISABLED")
        assertEquals("MULTI_VERSION_DISABLED", get_multi_version_status(host))
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"

        // DISABLED -> WRITE_ONLY -> READ_WRITE -> WRITE_ONLY -> DISABLED
        insert_data(tableName, 21, 30, [3,4])
        set_multi_version_status(host, "MULTI_VERSION_WRITE_ONLY")
        set_multi_version_status(host, "MULTI_VERSION_WRITE_ONLY")
        assertEquals("MULTI_VERSION_WRITE_ONLY", get_multi_version_status(host))
        insert_data(tableName, 31, 40, [4,5])
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"
        wait_snapshot_switch_status(host, "DISABLED")
        set_multi_version_status(host, "MULTI_VERSION_READ_WRITE")
        set_multi_version_status(host, "MULTI_VERSION_READ_WRITE")
        assertEquals("MULTI_VERSION_READ_WRITE", get_multi_version_status(host))
        insert_data(tableName, 41, 50, [5,6])
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"
        set_multi_version_status(host, "MULTI_VERSION_WRITE_ONLY")
        assertEquals("MULTI_VERSION_WRITE_ONLY", get_multi_version_status(host))
        insert_data(tableName, 51, 60, [6,7])
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"
        set_multi_version_status(host, "MULTI_VERSION_DISABLED")
        assertEquals("MULTI_VERSION_DISABLED", get_multi_version_status(host))
        assertEquals("UNSUPPORTED", get_snapshot_switch_status(host))
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"

        // DISABLED -> WRITE_ONLY -> READ_WRITE -> DISABLED
        insert_data(tableName, 61, 70, [7,8])
        set_multi_version_status(host, "MULTI_VERSION_WRITE_ONLY")
        set_multi_version_status(host, "MULTI_VERSION_WRITE_ONLY")
        assertEquals("MULTI_VERSION_WRITE_ONLY", get_multi_version_status(host))
        insert_data(tableName, 71, 80, [8,9])
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"
        wait_snapshot_switch_status(host, "DISABLED")
        set_multi_version_status(host, "MULTI_VERSION_READ_WRITE")
        set_multi_version_status(host, "MULTI_VERSION_READ_WRITE")
        assertEquals("MULTI_VERSION_READ_WRITE", get_multi_version_status(host))
        insert_data(tableName, 81, 90, [9,10])
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"
        set_multi_version_status(host, "MULTI_VERSION_DISABLED")
        assertEquals("MULTI_VERSION_DISABLED", get_multi_version_status(host))
        assertEquals("UNSUPPORTED", get_snapshot_switch_status(host))
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"

        // DISABLED -> WRITE_ONLY -> READ_WRITE -> DISABLED, with heavy schema change
        sql """
            ALTER TABLE ${tableName} ADD COLUMN k3 INT DEFAULT 1 AFTER k2,
                ADD COLUMN k4 INT DEFAULT 2 AFTER k3
        """
        set_multi_version_status(host, "MULTI_VERSION_WRITE_ONLY")
        set_multi_version_status(host, "MULTI_VERSION_WRITE_ONLY")
        assertEquals("MULTI_VERSION_WRITE_ONLY", get_multi_version_status(host))
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2, k3, k4"
        wait_alter_table_column_finished(tableName)
        wait_snapshot_switch_status(host, "DISABLED")

        sql """
            ALTER TABLE ${tableName} DROP COLUMN k4
        """
        set_multi_version_status(host, "MULTI_VERSION_READ_WRITE")
        set_multi_version_status(host, "MULTI_VERSION_READ_WRITE")
        assertEquals("MULTI_VERSION_READ_WRITE", get_multi_version_status(host))
        wait_alter_table_column_finished(tableName)
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2, k3"

        sql """
            ALTER TABLE ${tableName} DROP COLUMN k3
        """
        set_multi_version_status(host, "MULTI_VERSION_DISABLED")
        assertEquals("MULTI_VERSION_DISABLED", get_multi_version_status(host))
        assertEquals("UNSUPPORTED", get_snapshot_switch_status(host))
        wait_alter_table_column_finished(tableName)
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"
    }
}
