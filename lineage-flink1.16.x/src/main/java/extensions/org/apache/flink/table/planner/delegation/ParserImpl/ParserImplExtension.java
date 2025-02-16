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

package extensions.org.apache.flink.table.planner.delegation.ParserImpl;

import com.google.common.base.Preconditions;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.parse.CalciteParser;

import manifold.ext.rt.api.Extension;
import manifold.ext.rt.api.Jailbreak;
import manifold.ext.rt.api.This;

import java.util.List;

/**
 * 使用 manifold-ext 扩展 {@link ParserImpl} 以添加新方法。
 *
 * 该类通过注解 @Extension 扩展 Flink SQL 解析器 `ParserImpl`，
 * 以便提供更灵活的 SQL 解析和校验能力。
 * 主要方法包括：
 * - `parseSql`: 解析 SQL 语句并返回抽象语法树（AST）。
 * - `validate`: 校验 SQL 语法的合法性。
 *
 * @author: HamaWhite
 */
@Extension
public class ParserImplExtension {

    /**
     * 解析 SQL 语句并返回抽象语法树（AST）。
     *
     * 该方法调用 Flink 内部的 CalciteParser 进行 SQL 解析。
     * 由于 Flink SQL Client 允许以 `;` 结尾的 SQL 语句，因此使用 `parseSqlList` 方法进行解析，
     * 并确保 SQL 语句列表中仅包含一个语句，否则抛出异常。
     *
     * @param thiz      需要扩展的 `ParserImpl` 实例。
     * @param statement 需要解析的 SQL 语句字符串。
     * @return 解析后的 SQL 抽象语法树（AST）。
     * @throws org.apache.flink.table.api.SqlParserException 当 SQL 解析失败时抛出异常。
     */
    public static SqlNode parseSql(@This @Jailbreak ParserImpl thiz, String statement) {
        // 使用 @Jailbreak 注解可以访问 ParserImpl 类中的私有变量
        CalciteParser parser = thiz.calciteParserSupplier.get();

        // 由于 Flink SQL Client 允许 SQL 语句以 `;` 结尾，因此需要使用 parseSqlList 方法进行解析
        SqlNodeList sqlNodeList = parser.parseSqlList(statement);
        List<SqlNode> parsed = sqlNodeList.getList();

        // 仅支持单条 SQL 语句，若解析出的语句列表不止一条，则抛出异常
        Preconditions.checkArgument(parsed.size() == 1, "仅支持单条 SQL 语句");
        return parsed.get(0);
    }

    /**
     * 校验 SQL 语句的合法性。
     *
     * 该方法使用 Flink 内部的 `FlinkPlannerImpl` 进行 SQL 语法校验，
     * 确保 SQL 语句符合 Flink 语法规范。
     *
     * @param thiz    需要扩展的 `ParserImpl` 实例。
     * @param sqlNode 需要校验的 SQL 语法树（AST）。
     * @return 经过校验后的 SQL 语法树（AST）。
     */
    public static SqlNode validate(@This @Jailbreak ParserImpl thiz, SqlNode sqlNode) {
        // 使用 @Jailbreak 注解可以访问 ParserImpl 类中的私有变量
        FlinkPlannerImpl flinkPlanner = thiz.validatorSupplier.get();

        // 执行 SQL 校验
        return flinkPlanner.validate(sqlNode);
    }
}
