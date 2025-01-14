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

package com.hw.lineage.common.model;

import com.hw.lineage.common.enums.TableKind;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

/**
 * @description: TableInfo - 用于存储表的元数据信息，包括表名、表类型、注释、列信息及属性等。
 */
@Data
@NoArgsConstructor
@Accessors(chain = true)
public class TableInfo {

    /**
     * 表名称，用于标识表的名称。
     * 例如：`users` 或 `orders`
     */
    private String tableName;

    /**
     * 表类型，使用 TableKind 枚举表示表的种类。
     * 例如：
     * - `TABLE`：普通表
     * - `VIEW`：视图
     */
    private TableKind tableKind;

    /**
     * 表的注释，用于描述表的用途或信息。
     * 例如：`This table stores user information.`
     */
    private String comment;

    /**
     * 列信息列表，包含表中所有列的详细信息。
     * 每个列的元数据由 ColumnInfo 对象表示。
     * 例如：`[ColumnInfo(columnName="id", columnType="BIGINT", ...)]`
     */
    private List<ColumnInfo> columnList;

    /**
     * 表的属性集合，用于存储与表相关的配置项。
     * 键值对形式：
     * - Key: 属性名称，例如 `partitioned_by`
     * - Value: 属性值，例如 `country`
     * 例如：`{"partitioned_by": "country", "format": "parquet"}`
     */
    private Map<String, String> propertiesMap;
}
