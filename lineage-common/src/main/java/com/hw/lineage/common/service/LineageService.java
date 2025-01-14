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

package com.hw.lineage.common.service;

import com.hw.lineage.common.model.FunctionInfo;
import com.hw.lineage.common.model.FunctionResult;
import com.hw.lineage.common.model.LineageResult;
import com.hw.lineage.common.model.TableInfo;
import com.hw.lineage.common.plugin.Plugin;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @description: LinageService（数据血缘服务接口）
 * 提供对SQL语句的血缘分析、解析验证、执行、以及与数据目录相关的操作功能。
 */
public interface LineageService extends Plugin {

    /**
     * 分析输入SQL的字段级血缘关系。
     *
     * @param singleSql 输入的SQL语句
     * @return 包含字段血缘分析结果的列表
     */
    List<LineageResult> analyzeLineage(String singleSql);

    /**
     * 对SQL语句执行解析和验证操作。
     *
     * @param singleSql 输入的SQL语句
     */
    void parseValidate(String singleSql);

    /**
     * 执行单条SQL语句。
     *
     * @param singleSql 输入的SQL语句
     */
    void execute(String singleSql);

    /**
     * 分析SQL中使用的自定义函数。
     *
     * @param singleSql 输入的SQL语句
     * @return 包含函数分析结果的集合
     */
    Set<FunctionResult> analyzeFunction(String singleSql);

    /**
     * 从jar文件中解析函数信息，包括函数名称、格式、主类和描述。
     *
     * @param file 包含函数的jar文件
     * @return 包含函数信息的列表
     * @throws IOException 如果文件读取发生错误
     * @throws ClassNotFoundException 如果类加载失败
     */
    List<FunctionInfo> parseFunction(File file) throws IOException, ClassNotFoundException;

    /**
     * 获取指定目录中所有数据库的名称。
     *
     * @param catalogName 数据目录的名称
     * @return 包含数据库名称的列表
     */
    List<String> listDatabases(String catalogName);

    /**
     * 获取指定数据库下的所有表和视图名称。如果不存在，则返回空列表。
     *
     * @param catalogName 数据目录的名称
     * @param database 数据库的名称
     * @return 包含表和视图名称的列表
     * @throws Exception 如果操作失败
     */
    List<String> listTables(String catalogName, String database) throws Exception;

    /**
     * 获取指定数据库下的所有视图名称。如果不存在，则返回空列表。
     *
     * @param catalogName 数据目录的名称
     * @param database 数据库的名称
     * @return 包含视图名称的列表
     * @throws Exception 如果操作失败
     */
    List<String> listViews(String catalogName, String database) throws Exception;

    /**
     * 读取注册的表并返回表信息。
     *
     * @param catalogName 数据目录的名称
     * @param database 数据库的名称
     * @param tableName 表的名称
     * @return 表信息对象
     * @throws Exception 如果操作失败
     */
    TableInfo getTable(String catalogName, String database, String tableName) throws Exception;

    /**
     * 获取指定表的DDL（数据定义语言）。
     *
     * @param catalogName 数据目录的名称
     * @param database 数据库的名称
     * @param tableName 表的名称
     * @return 表的DDL字符串
     * @throws Exception 如果操作失败
     */
    String getTableDdl(String catalogName, String database, String tableName) throws Exception;

    /**
     * 删除指定的表。
     *
     * @param catalogName 数据目录的名称
     * @param database 数据库的名称
     * @param tableName 表的名称
     * @throws Exception 如果操作失败
     */
    void dropTable(String catalogName, String database, String tableName) throws Exception;
}

