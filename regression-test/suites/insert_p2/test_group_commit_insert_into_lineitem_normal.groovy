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


suite("test_group_commit_insert_into_lineitem_normal") {
    String[] file_array;
    def getFiles = { String dirName, int num->
        File[] datas = new File(dirName).listFiles()
        if (num != datas.length) {
            throw new Exception("num not equals,expect:" + num + " vs real:" + datas.length)
        }
        file_array = new String[datas.length];
        for (int i = 0; i < datas.length; i++) {
            file_array[i] = datas[i].getPath();
        }
        Arrays.sort(file_array);
    }
    def prepare = {
        def dataDir = "${context.config.cacheDataPath}/insert_into_lineitem_normal"
        File dir = new File(dataDir)
        if (!dir.exists()) {
            new File(dataDir).mkdir()
            logger.info("download lineitem")
            def download_file = """/usr/bin/curl ${getS3Url()}/regression/tpch/sf1/lineitem.tbl.1
--output ${dataDir}/lineitem.tbl.1""".execute().getText()
            def split_file = """split -l 60000 ${dataDir}/lineitem.tbl.1 ${dataDir}/""".execute().getText()
            def rm_file = """rm ${dataDir}/lineitem.tbl.1""".execute().getText()
        }
        getFiles(dataDir, 11)
        for (String s : file_array) {
            logger.info(s)
        }
    }
    def insert_table = "test_insert_into_lineitem_normal"
    // Set batch to 90 to avoid JDBC driver bug. Larger batch size may trigger JDBC send COM_STMT_FETCH command.
    def batch = 90;
    def count = 0;
    def total = 0;

    def getRowCount = { expectedRowCount, table_name ->
        def retry = 0
        while (retry < 60) {
            try {
                def rowCount = sql "select count(*) from ${table_name}"
                logger.info("rowCount: " + rowCount + ", retry: " + retry)
                if (rowCount[0][0] >= expectedRowCount) {
                    break
                }
            } catch (Exception e) {
                logger.info("select count get exception", e);
            }
            Thread.sleep(5000)
            retry++
        }
    }

    def create_insert_table = {
        // create table
        sql """ drop table if exists ${insert_table}; """

        sql """
   CREATE TABLE ${insert_table} (
    l_shipdate    DATEV2 NOT NULL,
    l_orderkey    bigint NOT NULL,
    l_linenumber  int not null,
    l_partkey     int NOT NULL,
    l_suppkey     int not null,
    l_quantity    decimalv3(15, 2) NOT NULL,
    l_extendedprice  decimalv3(15, 2) NOT NULL,
    l_discount    decimalv3(15, 2) NOT NULL,
    l_tax         decimalv3(15, 2) NOT NULL,
    l_returnflag  VARCHAR(1) NOT NULL,
    l_linestatus  VARCHAR(1) NOT NULL,
    l_commitdate  DATEV2 NOT NULL,
    l_receiptdate DATEV2 NOT NULL,
    l_shipinstruct VARCHAR(25) NOT NULL,
    l_shipmode     VARCHAR(10) NOT NULL,
    l_comment      VARCHAR(44) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`l_shipdate`, `l_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1"
);
        """
        sql """ set group_commit = async_mode; """
    }

    def do_insert_into = { exp_str, num ->
        def i = 0;
        while (true) {
            try {
                def result = insert_into_sql(exp_str, num);
                logger.info("result:" + result);
                break
            } catch (Exception e) {
                logger.info("got exception:" + e)
                logger.info("sql: " + exp_str)
                Thread.sleep(5000)
                context.reconnectFe()
                sql """ set group_commit = async_mode; """
            }
            i++;
            if (i >= 30) {
                throw new Exception("""fail to much time""")
            }
        }
    }

    def process = {
        for (String file : file_array) {
            logger.info("insert into file: " + file)
            BufferedReader reader;
            try {
                reader = new BufferedReader(new FileReader(file));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }

            String s = null;
            StringBuilder sb = null;
            count = 0;
            while (true) {
                try {
                    if (count == batch) {
                        sb.append(";");
                        String exp = sb.toString();
                        do_insert_into(exp, count);
                        count = 0;
                    }
                    s = reader.readLine();
                    if (s != null) {
                        if (count == 0) {
                            sb = new StringBuilder();
                            sb.append("insert into ${insert_table} (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment)VALUES");
                        }
                        if (count > 0) {
                            sb.append(",");
                        }
                        String[] array = s.split("\\|");
                        sb.append("(");
                        for (int i = 0; i < array.length; i++) {
                            sb.append("\"" + array[i] + "\"");
                            if (i != array.length - 1) {
                                sb.append(",");
                            }
                        }
                        sb.append(")");
                        count++;
                        total++;
                    } else if (count > 0) {
                        sb.append(";");
                        String exp = sb.toString();
                        do_insert_into(exp, count);
                        break;
                    } else {
                        break;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (reader != null) {
                reader.close();
            }
        }
        logger.info("total: " + total)
        getRowCount(total, insert_table)

        qt_sql """select count(*) from ${insert_table};"""
    }

    try {
        prepare()
        create_insert_table()
        for (int i = 0; i < 1; i++) {
            process()
        }
    } finally {
    }
}
