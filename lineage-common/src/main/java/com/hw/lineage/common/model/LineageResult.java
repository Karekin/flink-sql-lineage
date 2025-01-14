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

import com.hw.lineage.common.util.Constant;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @description: Result
 * @author: HamaWhite
 */
@Data
@Builder
@AllArgsConstructor
/**
 * @description: LineageResult - 用于存储字段血缘信息，包括源表与目标表之间的字段映射和转换信息。
 */
public class LineageResult {

    // 源表所属的 Catalog
    private String sourceCatalog;

    // 源表所属的数据库
    private String sourceDatabase;

    // 源表名称
    private String sourceTable;

    // 源字段名称
    private String sourceColumn;

    // 目标表所属的 Catalog
    private String targetCatalog;

    // 目标表所属的数据库
    private String targetDatabase;

    // 目标表名称
    private String targetTable;

    // 目标字段名称
    private String targetColumn;

    /**
     * 存储数据转换的表达式，表示源字段通过某种转换生成目标字段。
     */
    private String transform;

    /**
     * 构造函数，用于根据表路径和字段信息初始化 LineageResult。
     *
     * @param sourceTablePath 源表路径（格式：catalog.database.table）
     * @param sourceColumn    源字段名称
     * @param targetTablePath 目标表路径（格式：catalog.database.table）
     * @param targetColumn    目标字段名称
     * @param transform       转换表达式，描述源字段到目标字段的转换方式
     */
    public LineageResult(String sourceTablePath, String sourceColumn, String targetTablePath, String targetColumn,
                         String transform) {
        // 根据指定分隔符（例如 "."）解析表路径
        String[] sourceItems = sourceTablePath.split("\\" + Constant.DELIMITER);
        String[] targetItems = targetTablePath.split("\\" + Constant.DELIMITER);

        // 初始化源表和目标表的字段信息
        this.sourceCatalog = sourceItems[0];
        this.sourceDatabase = sourceItems[1];
        this.sourceTable = sourceItems[2];
        this.sourceColumn = sourceColumn;
        this.targetCatalog = targetItems[0];
        this.targetDatabase = targetItems[1];
        this.targetTable = targetItems[2];
        this.targetColumn = targetColumn;
        this.transform = transform;
    }

    /**
     * 构造函数，用于初始化 LineageResult（不包含转换表达式）。
     *
     * @param catalog      Catalog 名称
     * @param database     数据库名称
     * @param sourceTable  源表名称
     * @param sourceColumn 源字段名称
     * @param targetTable  目标表名称
     * @param targetColumn 目标字段名称
     */
    public LineageResult(String catalog, String database, String sourceTable, String sourceColumn, String targetTable,
                         String targetColumn) {
        this.sourceCatalog = catalog;
        this.sourceDatabase = database;
        this.sourceTable = sourceTable;
        this.sourceColumn = sourceColumn;
        this.targetCatalog = catalog;
        this.targetDatabase = database;
        this.targetTable = targetTable;
        this.targetColumn = targetColumn;
    }

    /**
     * 静态方法，用于批量构建字段血缘结果。
     *
     * @param catalog       Catalog 名称
     * @param database      数据库名称
     * @param expectedArray 字段血缘信息的二维数组，每个子数组格式为：
     *                      {源表名, 源字段名, 目标表名, 目标字段名, 可选的转换表达式}
     * @return 包含 LineageResult 的列表
     */
    public static List<LineageResult> buildResult(String catalog, String database, String[][] expectedArray) {
        return Stream.of(expectedArray)
                .map(e -> {
                    // 创建 LineageResult 对象
                    LineageResult result = new LineageResult(catalog, database, e[0], e[1], e[2], e[3]);
                    // 如果数组包含转换表达式，设置 transform 字段
                    if (e.length == 5) {
                        result.setTransform(e[4]);
                    }
                    return result;
                }).collect(Collectors.toList());
    }
}

