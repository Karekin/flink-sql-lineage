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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * @description: ColumnInfo - 用于存储表字段的元数据信息，包括字段名称、类型、注释、主键标识和水印信息。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ColumnInfo {

    /**
     * 字段名称，用于标识字段。
     * 例如：`id`
     */
    private String columnName;

    /**
     * 字段类型，表示字段的数据类型。
     * 例如：`BIGINT`, `VARCHAR(255)`
     */
    private String columnType;

    /**
     * 字段注释，用于说明字段的用途。
     * 例如：`Primary key of the table`
     */
    private String comment;

    /**
     * 是否为主键，标识该字段是否为表的主键。
     * true 表示是主键；false 表示不是主键。
     */
    private Boolean primaryKey;

    /**
     * 水印信息，用于定义流处理中的时间属性。
     * 例如：`WATERMARK FOR ts AS ts - INTERVAL '5' SECOND`
     */
    private String watermark;
}

