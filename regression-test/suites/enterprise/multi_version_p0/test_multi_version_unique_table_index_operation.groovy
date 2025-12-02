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

suite("test_multi_version_unique_table_index_operation", "snapshot,docker") {
    if (!isCloudMode()) {
        logger.info("Skip test_multi_version_unique_table_index_operation because not in cloud mode")
        return
    }

    def create_unique_table = { tableName ->
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                k1 INT,
                k2 INT,
                k3 VARCHAR(30),
                v1 INT
            ) ENGINE=OLAP
            UNIQUE KEY(k1, k2, k3)
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

    def index_types = [
        "INVERTED": { tableName, columnName ->
            sql """ ALTER TABLE ${tableName} ADD INDEX idx_${columnName}_inverted (${columnName}) USING INVERTED """
        },
        "BF": { tableName, columnName ->
            sql """ ALTER TABLE ${tableName} ADD INDEX idx_${columnName}_bf (${columnName}) USING BITMAP """
        },
        "NGBF": { tableName, columnName ->
            sql """ ALTER TABLE ${tableName} ADD INDEX idx_${columnName}_ngbf (${columnName}) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="1024") """
        }
    ]

    def operations = [
        "INSERT": { tableName, k1Val, k2Val, k3Val, v1Val ->
            sql """ INSERT INTO ${tableName} VALUES (${k1Val}, ${k2Val}, '${k3Val}', ${v1Val}) """
        },
        "DELETE": { tableName, k1Val, k2Val, k3Val, v1Val ->
            sql """ DELETE FROM ${tableName} WHERE k1 = ${k1Val} AND k2 = ${k2Val} """
        },
        "UPDATE": { tableName, k1Val, k2Val, k3Val, v1Val ->
            sql """ UPDATE ${tableName} SET v1 = ${v1Val} WHERE k1 = ${k1Val} AND k2 = ${k2Val} """
        }
    ]

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

    def wait_alter_table_column_finished = { tableName ->
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(2)).atMost(java.time.Duration.ofMinutes(3)).until {
            def result = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' AND State!='FINISHED'"
            return result.size() == 0 ? true : false
        }
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
        Awaitility.await().pollInterval(java.time.Duration.ofSeconds(2)).atMost(java.time.Duration.ofMinutes(1)).until {
            def status = get_snapshot_switch_status(host)
            return status == expected_status
        }
    }

    def test_combination = { host, indexType, operation, tableNum ->
        def tableName = "unique_table_${indexType.toLowerCase()}_${operation.toLowerCase()}_${tableNum}"
        
        logger.info("Testing UNIQUE table with index=${indexType}, operation=${operation}")
        
        create_unique_table(tableName)
        
        if (indexType == "NGBF") {
            index_types[indexType](tableName, "k3")
            wait_alter_table_column_finished(tableName)
        } else {
            index_types[indexType](tableName, "v1")
            wait_alter_table_column_finished(tableName)
        }
        
        sql """ INSERT INTO ${tableName} VALUES (1, 1, 'test', 10) """
        sql """ INSERT INTO ${tableName} VALUES (1, 2, 'test', 20) """
        
        assertEquals("MULTI_VERSION_DISABLED", get_multi_version_status(host))
        
        set_multi_version_status(host, "MULTI_VERSION_WRITE_ONLY")
        set_multi_version_status(host, "MULTI_VERSION_WRITE_ONLY")
        assertEquals("MULTI_VERSION_WRITE_ONLY", get_multi_version_status(host))
        
        operations[operation](tableName, 2, 3, "test", 30)
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"
        
        wait_snapshot_switch_status(host, "DISABLED")
        
        set_multi_version_status(host, "MULTI_VERSION_READ_WRITE")
        set_multi_version_status(host, "MULTI_VERSION_READ_WRITE")
        assertEquals("MULTI_VERSION_READ_WRITE", get_multi_version_status(host))
        
        operations[operation](tableName, 3, 4, "test", 40)
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"
        
        set_multi_version_status(host, "MULTI_VERSION_DISABLED")
        assertEquals("MULTI_VERSION_DISABLED", get_multi_version_status(host))
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"

        set_multi_version_status(host, "MULTI_VERSION_WRITE_ONLY")
        set_multi_version_status(host, "MULTI_VERSION_WRITE_ONLY")
        assertEquals("MULTI_VERSION_WRITE_ONLY", get_multi_version_status(host))

        operations[operation](tableName, 4, 5, "test", 50)
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"

        wait_snapshot_switch_status(host, "DISABLED")

        set_multi_version_status(host, "MULTI_VERSION_READ_WRITE")
        set_multi_version_status(host, "MULTI_VERSION_READ_WRITE")
        assertEquals("MULTI_VERSION_READ_WRITE", get_multi_version_status(host))

        operations[operation](tableName, 5, 6, "test", 60)
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"

        set_multi_version_status(host, "MULTI_VERSION_DISABLED")
        assertEquals("MULTI_VERSION_DISABLED", get_multi_version_status(host))
        qt_sql "SELECT * FROM ${tableName} ORDER BY k1, k2"
    }

    def cluster_name = "regression_test_unique_multi_version_table_index_operation"
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
        def msList = cluster.getMetaservices()
        def (ip, port) = msList[0].getHttpAddress()
        def host = "${ip}:${port}"

        def tableNum = 0
        for (indexType in index_types.keySet()) {
            for (operation in operations.keySet()) {
                test_combination(host, indexType, operation, tableNum++)
            }
        }
    }
}

