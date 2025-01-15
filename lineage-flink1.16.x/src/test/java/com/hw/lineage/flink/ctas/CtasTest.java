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

package com.hw.lineage.flink.ctas;

import com.hw.lineage.flink.basic.AbstractBasicTest;

import org.junit.Before;
import org.junit.Test;

/**
 * 测试用例来自 <a href="https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/sql/create/#as-select_statement">
 * flink-1.16-create-table-as-select</a>
 * <p>
 * 该测试类主要演示了如何在 Flink 中使用 CREATE TABLE AS SELECT (CTAS) 语句创建表并将来源表中的数据
 * 通过 SELECT 语句插入到新表中。本例中使用了一个 datagen_source 表作为数据源，并将筛选后的数据插入到
 * my_ctas_table 表中。
 *
 * @description: CtasTest
 * @author: HamaWhite
 */
public class CtasTest extends AbstractBasicTest {

    /**
     * 在每个测试方法执行之前都会先调用该方法，确保创建数据源表 datagen_source。
     * 这里使用了一个 @Before 注解来表明该方法会在每个测试执行前执行。
     */
    @Before
    public void createTable() {
        // 创建名为 datagen_source 的 DataGen 源表
        createTableOfDatagenSource();
    }

    /**
     * 测试使用 CTAS（CREATE TABLE AS SELECT） 的功能：
     * 1. 首先检查并删除已存在的目标表 my_ctas_table（如果存在）；
     * 2. 然后使用 CTAS 语句创建并插入数据；
     * 3. 最后调用 analyzeLineage 方法检查实际的字段血缘是否与预期匹配。
     */
    @Test
    public void testCtas() {
        // 如果存在 my_ctas_table，则先删除表
        context.execute("DROP TABLE IF EXISTS my_ctas_table ");

        // 定义 CTAS 语句，将 datagen_source 表中符合条件 mod(id,10)=0 的数据插入到 my_ctas_table 表中
        String ctasSql = "CREATE TABLE IF NOT EXISTS my_ctas_table            " +
                "WITH (                                                       " +
                "       'connector' = 'print'                                 " +
                ") AS                                                         " +
                "SELECT                                                       " +
                "       id                                                   ," +
                "       name                                                 ," +
                "       age                                                   " +
                "FROM                                                         " +
                "       datagen_source                                        " +
                "WHERE                                                        " +
                "       mod(id, 10) = 0                                       ";

        // 预期的字段血缘映射关系，以二维数组形式表示：
        // 比如 {"datagen_source", "id", "my_ctas_table", "id"} 表示
        // datagen_source 表的 id 字段映射到 my_ctas_table 表的 id 字段。
        String[][] expectedArray = {
                {"datagen_source", "id", "my_ctas_table", "id"},
                {"datagen_source", "name", "my_ctas_table", "name"},
                {"datagen_source", "age", "my_ctas_table", "age"}
        };

        // 调用分析血缘关系的方法，检查实际结果是否与预期匹配
        analyzeLineage(ctasSql, expectedArray);
    }

    /**
     * 创建名为 datagen_source 的 DataGen 表，包含三个字段：
     * 1. id (BIGINT)
     * 2. name (STRING)
     * 3. age (INT)
     *
     * 该表使用 'connector' = 'datagen' 表示数据以 DataGen 方式自动生成。
     */
    protected void createTableOfDatagenSource() {
        // 如果存在 datagen_source，则先删除表
        context.execute("DROP TABLE IF EXISTS datagen_source ");

        // 创建 datagen_source 表，并指定字段类型及 DataGen 连接器
        context.execute("CREATE TABLE IF NOT EXISTS datagen_source ( " +
                "       id              BIGINT                               ," +
                "       name            STRING                               ," +
                "       age             INT                                   " +
                ") WITH (                                                     " +
                "       'connector' = 'datagen'                               " +
                ")");
    }
}

