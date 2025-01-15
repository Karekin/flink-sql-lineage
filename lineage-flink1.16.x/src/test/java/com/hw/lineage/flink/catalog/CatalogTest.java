/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hw.lineage.flink.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.hw.lineage.common.enums.TableKind;
import com.hw.lineage.common.model.ColumnInfo;
import com.hw.lineage.common.model.TableInfo;
import com.hw.lineage.flink.LineageServiceImpl;

import org.apache.flink.table.api.TableException;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * 此单元测试依赖外部的 Hive 和 Mysql 环境，
 * 如果没有这些环境，请注释掉该测试类（添加 @Ignore 注解）
 *
 * @author: HamaWhite
 */
public class CatalogTest {

    // 日志记录器，用于输出日志信息
    private static final Logger LOG = LoggerFactory.getLogger(CatalogTest.class);

    // 数据库名称
    private static final String database = "lineage_db";

    // 表名称
    private static final String tableName = "ods_mysql_users_watermark";

    // LineageServiceImpl 的实例，负责执行具体的操作
    private final LineageServiceImpl context = new LineageServiceImpl();

    /**
     * 测试内存目录（Memory Catalog）
     */
    @Test
    public void testMemoryCatalog() {
        // 指定的内存目录名称
        String catalogName = "memory_catalog";
        // 使用内存目录
        useMemoryCatalog(catalogName);
        // 验证当前目录是否为指定的内存目录
        assertThat(context.getCurrentCatalog(), is(catalogName));
    }

    /**
     * 使用内存目录（Memory Catalog）的配置
     *
     * @param catalogName 内存目录的名称
     */
    private void useMemoryCatalog(String catalogName) {
        // 创建内存目录并设置默认数据库
        context.execute(String.format(
                "CREATE CATALOG %s with ( 'type'='generic_in_memory','default-database'='" + database + "' )",
                catalogName));
        // 切换到指定的内存目录
        context.execute(String.format("USE CATALOG %s", catalogName));
    }

    /**
     * 测试 JDBC 目录（需要依赖外部 MySQL 环境）
     */
    @Test
    @Ignore("depends on the external Mysql environment")
    public void testJdbcCatalog() {
        // 指定的 JDBC 目录名称
        String catalogName = "jdbc_catalog";
        // 使用 JDBC 目录
        useJdbcCatalog(catalogName);
        // 验证当前目录是否为指定的 JDBC 目录
        assertThat(context.getCurrentCatalog(), is(catalogName));
    }

    /**
     * 使用 JDBC 目录的配置
     *
     * @param catalogName JDBC 目录的名称
     */
    private void useJdbcCatalog(String catalogName) {
        // 创建 JDBC 目录时，数据库需要预先创建，否则会报错
        context.execute("CREATE CATALOG " + catalogName + " with (  " +
                "       'type' = 'jdbc'                                     ," +
                "       'default-database' = '" + database + "'             ," +
                "       'username' = 'root'                                 ," +
                "       'password' = 'root@123456'                          ," +
                "       'base-url' = 'jdbc:mysql://192.168.90.150:3306'      " +
                ")");
        // 切换到指定的 JDBC 目录
        context.execute(String.format("USE CATALOG %s", catalogName));
    }

    /**
     * 测试 Hive 目录（需要依赖外部 Hive 环境）
     */
    @Test
    @Ignore("depends on the external Hive environment")
    public void testHiveCatalog() {
        // 指定的 Hive 目录名称
        String catalogName = "hive_catalog";
        // 使用 Hive 目录
        useHiveCatalog(catalogName);
        // 验证当前目录是否为指定的 Hive 目录
        assertThat(context.getCurrentCatalog(), is(catalogName));
    }

    /**
     * 使用 Hive 目录的配置
     *
     * @param catalogName Hive 目录的名称
     */
    private void useHiveCatalog(String catalogName) {
        // 创建 Hive 目录时，数据库需要预先创建，否则会报错
        context.execute("CREATE CATALOG " + catalogName + " with (          " +
                "       'type' = 'hive'                                              ," +
                "       'default-database' = '" + database + "'                      ," +
                "       'hive-conf-dir' = '../data/hive-conf-dir'                    ," +
                "       'hive-version' = '3.1.2'                                      " +
                ")");
        // 切换到指定的 Hive 目录
        context.execute(String.format("USE CATALOG %s", catalogName));
    }

    /**
     * 测试查询内存目录的信息
     */
    @Test
    public void testQueryMemoryCatalogInfo() throws Exception {
        // 指定的内存目录名称
        String catalogName = "memory_catalog";
        // 使用内存目录
        useMemoryCatalog(catalogName);
        // 检查查询目录信息的方法
        checkQueryCatalogInfo(catalogName);
    }

    /**
     * 测试查询 JDBC 目录的信息（需要依赖外部 MySQL 环境）
     */
    @Test
    @Ignore("depends on the external Mysql environment")
    public void testQueryJdbcCatalogInfo() throws Exception {
        // 指定的 JDBC 目录名称
        String catalogName = "jdbc_catalog";
        // 使用 JDBC 目录
        useJdbcCatalog(catalogName);

        // 验证在未正确创建表的情况下抛出预期的异常
        assertThrows(
                String.format("Could not execute CreateTable in path `%s`.`%s`.`%s`", catalogName, database, tableName),
                TableException.class, () -> checkQueryCatalogInfo(catalogName));
    }

    /**
     * 测试查询 Hive Catalog 信息的方法（依赖外部 Hive 环境）
     */
    @Test
    @Ignore("depends on the external Hive environment")
    public void testQueryHiveCatalogInfo() throws Exception {
        // 指定 Hive 目录的名称
        String catalogName = "hive_catalog";
        // 使用 Hive 目录
        useHiveCatalog(catalogName);
        // 检查查询 Hive Catalog 信息的方法
        checkQueryCatalogInfo(catalogName);
    }

    /**
     * 检查并验证指定目录中的数据库和表的信息
     *
     * @param catalogName 目录名称
     * @throws Exception 如果执行过程中发生异常
     */
    private void checkQueryCatalogInfo(String catalogName) throws Exception {
        // 创建测试表 `ods_mysql_users_watermark`
        createTableOfOdsMysqlUsersWatermark();
        // 验证指定目录下是否包含测试数据库
        assertThat(context.listDatabases(catalogName), hasItem(database));
        // 验证指定目录和数据库下是否包含测试表
        assertThat(context.listTables(catalogName, database), hasItem(tableName));
        // 验证指定目录和数据库下是否没有视图
        assertEquals(Collections.emptyList(), context.listViews(catalogName, database));

        // 构建期望的表信息对象
        TableInfo expectedTableInfo = new TableInfo()
                .setTableName(tableName) // 设置表名称
                .setTableKind(TableKind.TABLE) // 设置表类型为 TABLE
                .setComment("Users Table") // 设置表注释
                .setColumnList( // 定义表的列信息
                        ImmutableList.of(
                                new ColumnInfo("id", "BIGINT", "", true, ""), // id 列，主键，类型为 BIGINT
                                new ColumnInfo("name", "STRING", "", false, ""), // name 列，类型为 STRING
                                new ColumnInfo("birthday", "TIMESTAMP(3)", "", false, ""), // birthday 列，类型为 TIMESTAMP(3)
                                new ColumnInfo("ts", "TIMESTAMP(3)", "", false, "[`ts` - INTERVAL '5' SECOND]"), // ts 列，带时间戳的水印定义
                                /**
                                 * TODO 优化：应为 PROCTIME()，但当前表示为 [PROCTIME()]
                                 * 因为不同 Expression 子类的 asSummaryString 方法有所不同
                                 */
                                new ColumnInfo("proc_time", "[PROCTIME()]", "", false, "")))
                .setPropertiesMap( // 设置表的属性
                        ImmutableMap.of(
                                "password", "xxx", // 数据库密码
                                "hostname", "127.0.0.1", // 主机名
                                "server-time-zone", "Asia/Shanghai", // 时区
                                "connector", "mysql-cdc", // 使用的连接器
                                "port", "3306", // 数据库端口
                                "database-name", "demo", // 数据库名称
                                "table-name", "users", // 表名称
                                "username", "root")); // 数据库用户名

        // 获取实际的表信息并验证是否与期望一致
        TableInfo tableInfo = context.getTable(catalogName, database, tableName);
        LOG.info("tableInfo: {}", tableInfo);
        assertEquals(expectedTableInfo, tableInfo);

        // 定义期望的表 DDL 语句
        String expectedTableDdl = "CREATE TABLE `ods_mysql_users_watermark` (\n" +
                "  `id` BIGINT NOT NULL,\n" +
                "  `name` VARCHAR(2147483647),\n" +
                "  `birthday` TIMESTAMP(3),\n" +
                "  `ts` TIMESTAMP(3),\n" +
                "  `proc_time` AS PROCTIME(),\n" +
                "  WATERMARK FOR `ts` AS `ts` - INTERVAL '5' SECOND,\n" +
                "  CONSTRAINT `PK_3386` PRIMARY KEY (`id`) NOT ENFORCED\n" +
                ") COMMENT 'Users Table'\n" +
                "WITH (\n" +
                "  'hostname' = '127.0.0.1',\n" +
                "  'password' = 'xxx',\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'port' = '3306',\n" +
                "  'database-name' = 'demo',\n" +
                "  'server-time-zone' = 'Asia/Shanghai',\n" +
                "  'table-name' = 'users',\n" +
                "  'username' = 'root'\n" +
                ")\n";

        // 获取实际的表 DDL 语句并验证是否与期望一致
        String tableDdl = context.getTableDdl(catalogName, database, tableName);
        LOG.info("tableDdl: {}", tableDdl);
        assertEquals(expectedTableDdl, tableDdl);
    }

    /**
     * 创建测试表 `ods_mysql_users_watermark`
     */
    private void createTableOfOdsMysqlUsersWatermark() {
        // 如果表已存在，则删除表
        context.execute("DROP TABLE IF EXISTS " + tableName);

        // 创建测试表并定义其 schema 和属性
        context.execute("CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "       id                  BIGINT PRIMARY KEY NOT ENFORCED ," + // 主键列 id，类型为 BIGINT
                "       name                STRING  COMMENT 'User Name'     ," + // 列 name，类型为 STRING，带注释
                "       birthday            TIMESTAMP(3)                    ," + // 列 birthday，类型为 TIMESTAMP(3)
                "       ts                  TIMESTAMP(3)                    ," + // 列 ts，类型为 TIMESTAMP(3)
                "       proc_time as proctime()                             ," + // 列 proc_time，定义为处理时间
                "       WATERMARK FOR ts AS ts - INTERVAL '5' SECOND         " + // 定义 ts 列的水印
                " ) " +
                " COMMENT 'Users Table' " + // 表注释
                " WITH ( " +
                "       'connector' = 'mysql-cdc'            ," + // 使用 mysql-cdc 连接器
                "       'hostname'  = '127.0.0.1'            ," + // 主机名
                "       'port'      = '3306'                 ," + // 端口号
                "       'username'  = 'root'                 ," + // 用户名
                "       'password'  = 'xxx'                  ," + // 密码
                "       'server-time-zone' = 'Asia/Shanghai' ," + // 时区
                "       'database-name' = 'demo'             ," + // 数据库名称
                "       'table-name'    = 'users' " + // 表名称
                ")");
    }

}
