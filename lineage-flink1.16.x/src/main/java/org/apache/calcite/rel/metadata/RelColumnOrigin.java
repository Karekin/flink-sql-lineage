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

package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.RelOptTable;

/**
 * 基于 Calcite 源码 org.apache.calcite.rel.metadata.RelColumnOrigin 修改。
 * <p>
 * 修改点：
 * 1. 添加了 `transform` 字段及相关代码，用于存储字段转换表达式。
 *
 * @description: RelColumnOrigin 是一个数据结构，用于描述由关系表达式生成的输出列的来源。
 * 它可以追溯列的来源表及相关信息。
 * @author: HamaWhite
 */
public class RelColumnOrigin {

    // ~ Instance fields --------------------------------------------------------

    // 列的来源表
    private final RelOptTable originTable;

    // 列在来源表中的索引（0-based）
    private final int iOriginColumn;

    // 是否为派生列（例如计算字段）
    private final boolean isDerived;

    /**
     * 存储字段转换的表达式，
     * 用于记录目标字段由来源表中的字段通过何种表达式转换得到。 TODO 算子级血缘的突破口
     */
    private String transform;

    // ~ Constructors -----------------------------------------------------------

    /**
     * 构造函数，用于初始化一个 RelColumnOrigin 对象（不带 transform 字段）。
     *
     * @param originTable   来源表
     * @param iOriginColumn 来源表中列的索引
     * @param isDerived     是否为派生列
     */
    public RelColumnOrigin(RelOptTable originTable, int iOriginColumn, boolean isDerived) {
        this.originTable = originTable;
        this.iOriginColumn = iOriginColumn;
        this.isDerived = isDerived;
    }

    /**
     * 构造函数，用于初始化一个 RelColumnOrigin 对象（带 transform 字段）。
     *
     * @param originTable   来源表
     * @param iOriginColumn 来源表中列的索引
     * @param isDerived     是否为派生列
     * @param transform     字段转换表达式
     */
    public RelColumnOrigin(RelOptTable originTable, int iOriginColumn, boolean isDerived, String transform) {
        this.originTable = originTable;
        this.iOriginColumn = iOriginColumn;
        this.isDerived = isDerived;
        this.transform = transform;
    }

    // ~ Methods ----------------------------------------------------------------

    /**
     * 返回来源表。
     *
     * @return 来源表 {@link RelOptTable}
     */
    public RelOptTable getOriginTable() {
        return originTable;
    }

    /**
     * 返回来源表中列的索引（0-based）。
     * 如果关系表达式已经执行了 UDT（用户定义类型）展开操作，
     * 则返回值可能是展开或未展开的列索引。
     *
     * @return 列的索引
     */
    public int getOriginColumnOrdinal() {
        return iOriginColumn;
    }

    /**
     * 判断该列是否为派生列。
     * 例如：
     * - 查询语句 `select a+b as c, d as e from t` 中：
     * - 列 c 是派生列，来源于 a 和 b。
     * - 列 e 是直接来源于 d 的非派生列。
     *
     * @return 如果是派生列返回 true；否则返回 false。
     */
    public boolean isDerived() {
        return isDerived;
    }

    /**
     * 返回字段转换的表达式。
     *
     * @return 转换表达式
     */
    public String getTransform() {
        return transform;
    }

    /**
     * 判断两个 RelColumnOrigin 对象是否相等。
     *
     * @param obj 要比较的对象
     * @return 如果两个对象的来源表、列索引和派生属性相等，则返回 true；否则返回 false。
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RelColumnOrigin)) {
            return false;
        }
        RelColumnOrigin other = (RelColumnOrigin) obj;
        return originTable.getQualifiedName().equals(other.originTable.getQualifiedName())
                && (iOriginColumn == other.iOriginColumn)
                && (isDerived == other.isDerived);
    }

    /**
     * 计算当前对象的哈希值。
     *
     * @return 哈希值
     */
    @Override
    public int hashCode() {
        return originTable.getQualifiedName().hashCode()
                + iOriginColumn + (isDerived ? 313 : 0);
    }
}

