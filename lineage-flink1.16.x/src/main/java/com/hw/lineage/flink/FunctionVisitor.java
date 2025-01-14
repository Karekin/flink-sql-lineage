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

package com.hw.lineage.flink;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.flink.table.catalog.UnresolvedIdentifier;

import java.util.ArrayList;
import java.util.List;

/**
 * @description: FunctionVisitor - 用于遍历 SQL 抽象语法树 (AST) 并提取函数信息
 * @author: HamaWhite
 */
public class FunctionVisitor extends SqlBasicVisitor<Void> {

    // 用于存储在 SQL 中发现的函数列表
    private final List<UnresolvedIdentifier> functionList = new ArrayList<>();

    /**
     * 访问 SQL 调用节点
     *
     * @param call 表示 SQL 调用的节点（例如函数调用）
     * @return 空值（Void），表示无需返回具体数据
     */
    @Override
    public Void visit(SqlCall call) {
        // 判断当前节点是否是基本调用 (SqlBasicCall) 且操作符是函数 (SqlFunction)
        if (call instanceof SqlBasicCall && call.getOperator() instanceof SqlFunction) {
            // 获取函数操作符
            SqlFunction function = (SqlFunction) call.getOperator();
            // 获取函数名称
            SqlIdentifier opName = function.getNameAsId();

            // 将函数名称解析为未解析标识符并添加到函数列表中
            functionList.add(UnresolvedIdentifier.of(opName.names));
        }
        // 调用父类的 visit 方法以继续遍历其他节点
        return super.visit(call);
    }

    /**
     * 获取函数列表
     *
     * @return 未解析标识符（函数名称）的列表
     */
    public List<UnresolvedIdentifier> getFunctionList() {
        return functionList;
    }
}

