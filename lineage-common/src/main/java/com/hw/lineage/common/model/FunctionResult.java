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

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @description: FunctionResult
 * @author: HamaWhite
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
/**
 * @description: FunctionResult - 用于存储函数的相关信息，包括 Catalog 名称、数据库名称和函数名称。
 */
public class FunctionResult {

    // Catalog 名称，表示函数所属的 Catalog
    private String catalogName;

    // 数据库名称，表示函数所属的数据库
    private String database;

    // 函数名称
    private String functionName;

    /**
     * 静态方法，用于构建一组 FunctionResult 对象
     *
     * @param catalog      Catalog 名称
     * @param database     数据库名称
     * @param expectedArray 函数名称数组，表示需要构建的函数名称
     * @return 包含所有 FunctionResult 对象的集合
     */
    public static Set<FunctionResult> buildResult(String catalog, String database, String[] expectedArray) {
        return Stream.of(expectedArray) // 将函数名称数组转换为流
                .map(e -> new FunctionResult(catalog, database, e)) // 为每个函数名称创建 FunctionResult 对象
                .collect(Collectors.toSet()); // 将结果收集为 Set 集合
    }
}

