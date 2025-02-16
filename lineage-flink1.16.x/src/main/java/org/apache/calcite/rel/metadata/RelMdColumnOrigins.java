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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.util.BuiltInMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.hw.lineage.common.util.Constant.DELIMITER;
import static com.hw.lineage.common.util.Constant.INITIAL_CAPACITY;

/**
 * 基于 Calcite 源码 org.apache.calcite.rel.metadata.RelMdColumnOrigins 修改的类。
 *
 * <p>修改点：
 * <ol>
 *  <li>支持 Lookup Join：新增 `getColumnOrigins(Snapshot rel, RelMetadataQuery mq, int iOutputColumn)` 方法。
 *  <li>支持 Watermark：新增 `getColumnOrigins(SingleRel rel, RelMetadataQuery mq, int iOutputColumn)` 方法。
 *  <li>支持表函数：新增 `getColumnOrigins(Correlate rel, RelMetadataQuery mq, int iOutputColumn)` 方法。
 *  <li>支持复杂事件处理 (CEP)：新增 `getColumnOrigins(Match rel, RelMetadataQuery mq, int iOutputColumn)` 方法。
 *  <li>支持字段转换：新增 `createDerivedColumnOrigins` 方法，处理字段的转换逻辑。
 *  <li>支持字段为 `LOCALTIMESTAMP`：修改 `getColumnOrigins(Project rel, RelMetadataQuery mq, int iOutputColumn)` 方法。
 *  <li>支持 PROCTIME() 作为首字段：新增 `computeIndexWithOffset` 方法。
 *  <li>支持表值函数 (TVF)：修改 `getColumnOrigins(TableFunctionScan rel, RelMetadataQuery mq, int iOutputColumn)` 方法。
 * </ol>
 *
 * @description: 提供了 {@link RelMetadataQuery#getColumnOrigins} 的默认实现，
 *               用于标准逻辑代数的列血缘追踪。
 * @author: HamaWhite
 */
public class RelMdColumnOrigins implements MetadataHandler<BuiltInMetadata.ColumnOrigin> {

    private static final Logger LOG = LoggerFactory.getLogger(RelMdColumnOrigins.class);

    // 正则表达式：匹配以 $ 开头的变量（例如 $0, $1）
    private final Pattern pattern = Pattern.compile("\\$[\\w.]+");

    /**
     * 提供对列血缘元数据的支持。
     */
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.COLUMN_ORIGIN.method, new RelMdColumnOrigins());

    // ~ Constructors -----------------------------------------------------------

    // 构造函数，声明为私有，仅通过反射调用。
    private RelMdColumnOrigins() {
    }

    // ~ Methods ----------------------------------------------------------------

    // 获取当前元数据定义的类型（列血缘分析的定义）
    public MetadataDef<BuiltInMetadata.ColumnOrigin> getDef() {
        return BuiltInMetadata.ColumnOrigin.DEF;
    }

    /**
     * 获取 Aggregate（聚合操作）的输出列血缘信息。
     *
     * @param rel          Aggregate 类型的关系表达式
     * @param mq           元数据查询工具
     * @param iOutputColumn 输出列的索引
     * @return 输出列的血缘信息集合
     */
    public Set<RelColumnOrigin> getColumnOrigins(Aggregate rel, RelMetadataQuery mq, int iOutputColumn) {
        // 判断输出列是分组列还是聚合列
        if (iOutputColumn < rel.getGroupCount()) {
            // 如果输出列是分组列
            // 根据分组列的索引直接返回来源列的血缘信息
            return mq.getColumnOrigins(
                    rel.getInput(), // 聚合操作的输入关系
                    rel.getGroupSet().asList().get(iOutputColumn) // 获取对应的分组列索引
            );
        }

        // 如果输出列是聚合列
        // 获取对应的聚合函数调用
        AggregateCall call = rel.getAggCallList().get(iOutputColumn - rel.getGroupCount());
        final Set<RelColumnOrigin> set = new LinkedHashSet<>();

        // 遍历聚合函数的所有参数，添加输入列的血缘信息
        for (Integer iInput : call.getArgList()) {
            set.addAll(mq.getColumnOrigins(rel.getInput(), iInput));
        }

        // 将输入列的信息标记为派生列，并附加聚合函数的转换逻辑
        return createDerivedColumnOrigins(set, call);
    }


    /**
     * 获取 Join 操作的输出列血缘信息。
     *
     * @param rel          Join 类型的关系表达式
     * @param mq           元数据查询工具
     * @param iOutputColumn 输出列的索引
     * @return 输出列的血缘信息集合
     */
    public Set<RelColumnOrigin> getColumnOrigins(Join rel, RelMetadataQuery mq, int iOutputColumn) {
        // 获取左表的列数
        int nLeftColumns = rel.getLeft().getRowType().getFieldList().size();
        Set<RelColumnOrigin> set; // 存储输出列的血缘信息
        boolean derived = false;  // 标记是否为派生列

        // 判断输出列属于左表还是右表
        if (iOutputColumn < nLeftColumns) {
            // 如果输出列属于左表
            set = mq.getColumnOrigins(rel.getLeft(), iOutputColumn); // 获取左表列的血缘信息

            // 如果 Join 类型为左外连接或全外连接，则左表的 NULL 值会导致列派生
            if (rel.getJoinType().generatesNullsOnLeft()) {
                derived = true; // 标记为派生列
            }
        } else {
            // 如果输出列属于右表
            set = mq.getColumnOrigins(rel.getRight(), iOutputColumn - nLeftColumns); // 获取右表列的血缘信息

            // 如果 Join 类型为右外连接或全外连接，则右表的 NULL 值会导致列派生
            if (rel.getJoinType().generatesNullsOnRight()) {
                derived = true; // 标记为派生列
            }
        }

        // 如果列因外连接产生了 NULL，则将其标记为派生列
        if (derived) {
            set = createDerivedColumnOrigins(set); // 创建派生列血缘信息
        }
        return set;
    }

    /**
     * 支持表函数的字段血缘分析。
     *
     * @param rel          关系表达式（Correlate 类型），表示左表与表函数的关联
     * @param mq           元数据查询工具，用于获取列血缘信息
     * @param iOutputColumn 输出列的索引
     * @return 输出列的血缘信息集合
     */
    public Set<RelColumnOrigin> getColumnOrigins(Correlate rel, RelMetadataQuery mq, int iOutputColumn) {
        // 获取左表的字段名列表
        List<String> fieldNameList = rel.getLeft().getRowType().getFieldNames();
        int nLeftColumns = fieldNameList.size(); // 左表字段数量

        // 如果输出列属于左表
        if (iOutputColumn < nLeftColumns) {
            return mq.getColumnOrigins(rel.getLeft(), iOutputColumn);
        } else {
            // 如果右表是 TableFunctionScan 类型（表函数扫描）
            if (rel.getRight() instanceof TableFunctionScan) {
                final Set<RelColumnOrigin> set = new LinkedHashSet<>();

                // 遍历左表的所需列，添加其血缘信息
                for (Integer iInput : rel.getRequiredColumns().asList()) {
                    set.addAll(mq.getColumnOrigins(rel.getLeft(), iInput));
                }

                // 获取表函数配置的字段转换信息
                TableFunctionScan tableFunctionScan = (TableFunctionScan) rel.getRight();
                String transform = computeTransform(set, tableFunctionScan.getCall()) // 转换表达式
                        + DELIMITER
                        + tableFunctionScan.getRowType().getFieldNames().get(iOutputColumn - nLeftColumns); // 右表字段名

                // 创建包含转换信息的派生字段血缘
                return createDerivedColumnOrigins(set, transform);
            }

            // 如果右表不是 TableFunctionScan，则直接获取右表的列血缘
            return mq.getColumnOrigins(rel.getRight(), iOutputColumn - nLeftColumns);
        }
    }

    /**
     * 支持集合操作（如 UNION、INTERSECT）的字段血缘分析。
     *
     * @param rel          关系表达式（SetOp 类型），表示集合操作
     * @param mq           元数据查询工具，用于获取列血缘信息
     * @param iOutputColumn 输出列的索引
     * @return 输出列的血缘信息集合
     */
    public Set<RelColumnOrigin> getColumnOrigins(SetOp rel, RelMetadataQuery mq, int iOutputColumn) {
        final Set<RelColumnOrigin> set = new LinkedHashSet<>();

        // 遍历集合操作的所有输入
        for (RelNode input : rel.getInputs()) {
            Set<RelColumnOrigin> inputSet = mq.getColumnOrigins(input, iOutputColumn);
            if (inputSet == null) {
                // 如果某个输入的血缘信息为空，则返回空集合
                return Collections.emptySet();
            }
            set.addAll(inputSet); // 添加输入的血缘信息
        }
        return set;
    }

    /**
     * 支持 Lookup Join 的字段血缘分析。
     *
     * @param rel          关系表达式（Snapshot 类型），表示 Lookup Join
     * @param mq           元数据查询工具，用于获取列血缘信息
     * @param iOutputColumn 输出列的索引
     * @return 输出列的血缘信息集合
     */
    public Set<RelColumnOrigin> getColumnOrigins(Snapshot rel, RelMetadataQuery mq, int iOutputColumn) {
        // 直接返回输入关系表达式的列血缘信息
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
    }

    /**
     * 支持 Watermark 的字段血缘分析。
     *
     * @param rel          关系表达式（SingleRel 类型），表示 Watermark
     * @param mq           元数据查询工具，用于获取列血缘信息
     * @param iOutputColumn 输出列的索引
     * @return 输出列的血缘信息集合
     */
    public Set<RelColumnOrigin> getColumnOrigins(SingleRel rel, RelMetadataQuery mq, int iOutputColumn) {
        // 直接返回输入关系表达式的列血缘信息
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
    }


    /**
     * 支持来源表中新生成字段（例如使用 LOCALTIMESTAMP 函数创建的字段）的血缘追踪。
     */
    public Set<RelColumnOrigin> getColumnOrigins(Project rel, final RelMetadataQuery mq, int iOutputColumn) {
        final RelNode input = rel.getInput(); // 获取输入节点
        RexNode rexNode = rel.getProjects().get(iOutputColumn); // 获取当前输出列的投影表达式

        // 如果是直接引用输入列
        if (rexNode instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) rexNode; // 转换为直接引用
            int index = inputRef.getIndex(); // 获取引用的列索引

            // 如果输入是 TableScan，则需要计算偏移量
            if (input instanceof TableScan) {
                index = computeIndexWithOffset(rel.getProjects(), inputRef.getIndex(), iOutputColumn);
            }
            return mq.getColumnOrigins(input, index); // 获取输入列的血缘信息
        }
        // 如果是 TableScan 且表达式是无操作数的函数（如 LOCALTIMESTAMP）
        else if (input instanceof TableScan && rexNode.getClass().equals(RexCall.class)
                && ((RexCall) rexNode).getOperands().isEmpty()) {
            return mq.getColumnOrigins(input, iOutputColumn); // 返回输入列的血缘信息
        }

        // 其他情况：可能是从多个列派生的复杂表达式
        final Set<RelColumnOrigin> set = getMultipleColumns(rexNode, input, mq); // 获取多个来源列
        return createDerivedColumnOrigins(set, rexNode); // 创建派生列血缘信息
    }

    /**
     * 计算索引偏移量，用于处理包含特殊字段（如 LOCALTIMESTAMP）的场景。
     */
    private int computeIndexWithOffset(List<RexNode> projects, int baseIndex, int iOutputColumn) {
        int offset = 0;

        // 遍历所有列，计算到当前列的偏移量
        for (int index = 0; index < iOutputColumn; index++) {
            RexNode rexNode = projects.get(index);
            // 如果列是无操作数的函数（如 LOCALTIMESTAMP），则增加偏移量
            if ((rexNode.getClass().equals(RexCall.class) && ((RexCall) rexNode).getOperands().isEmpty())) {
                offset += 1;
            }
        }
        return baseIndex + offset; // 返回调整后的列索引
    }

    /**
     * 支持复杂事件处理（CEP）的字段血缘追踪。
     * - 第一个列是 PARTITION BY 字段。
     * - 其他列来自 Match 的 MEASURES 定义。
     */
    public Set<RelColumnOrigin> getColumnOrigins(Match rel, RelMetadataQuery mq, int iOutputColumn) {
        final RelNode input = rel.getInput(); // 获取输入节点
        List<String> fieldNameList = input.getRowType().getFieldNames(); // 获取输入列名列表
        String fieldName = rel.getRowType().getFieldNames().get(iOutputColumn); // 获取当前列名

        // 1. 获取 PARTITION BY 的列名集合
        Set<String> partitionKeySet = rel.getPartitionKeys().toList()
                .stream()
                .map(fieldNameList::get)
                .collect(Collectors.toSet());

        // 2. 如果当前列属于 PARTITION BY 的字段，直接返回其血缘信息
        if (partitionKeySet.contains(fieldName)) {
            return mq.getColumnOrigins(input, fieldNameList.indexOf(fieldName));
        }

        // 3. 其他列来自 Match 的 MEASURES 定义
        RexNode rexNode = rel.getMeasures().get(fieldName); // 获取当前列的定义
        RexPatternFieldRef rexPatternFieldRef = searchRexPatternFieldRef(rexNode); // 搜索字段引用

        if (rexPatternFieldRef != null) {
            final Set<RelColumnOrigin> set = mq.getColumnOrigins(input, rexPatternFieldRef.getIndex()); // 获取输入血缘
            if (rexNode instanceof RexCall) {
                // 如果是函数调用，记录操作数信息
                return createDerivedColumnOrigins(set, ((RexCall) rexNode).getOperands().get(0));
            } else {
                // 否则，直接返回派生血缘信息
                return createDerivedColumnOrigins(set);
            }
        }

        // 4. 不支持的情况，返回空
        LOG.warn("无法解析列血缘信息，关系表达式:[{}], 输出列索引:[{}]", rel, iOutputColumn);
        return Collections.emptySet();
    }

    /**
     * 递归搜索 RexNode 中的字段引用（RexPatternFieldRef）。
     */
    private RexPatternFieldRef searchRexPatternFieldRef(RexNode rexNode) {
        if (rexNode instanceof RexCall) {
            RexNode operand = ((RexCall) rexNode).getOperands().get(0); // 获取第一个操作数
            if (operand instanceof RexPatternFieldRef) {
                return (RexPatternFieldRef) operand; // 如果是字段引用，返回
            } else {
                // 递归搜索其他操作数
                return searchRexPatternFieldRef(operand);
            }
        }
        return null; // 如果未找到，返回 null
    }


    /**
     * 获取 Calc 类型的列血缘信息。
     * Calc 是一种合并了投影和过滤操作的优化节点。
     *
     * @param rel          Calc 类型的关系表达式
     * @param mq           元数据查询工具
     * @param iOutputColumn 输出列的索引
     * @return 输出列的血缘信息集合
     */
    public Set<RelColumnOrigin> getColumnOrigins(Calc rel, final RelMetadataQuery mq, int iOutputColumn) {
        final RelNode input = rel.getInput(); // 获取 Calc 的输入节点

        // 使用 RexShuttle 替换本地引用（LocalRef）
        final RexShuttle rexShuttle = new RexShuttle() {
            @Override
            public RexNode visitLocalRef(RexLocalRef localRef) {
                return rel.getProgram().expandLocalRef(localRef); // 展开本地引用
            }
        };

        // 获取展开后的投影列表
        List<RexNode> projects = new ArrayList<>(rexShuttle.apply(rel.getProgram().getProjectList()));

        final RexNode rexNode = projects.get(iOutputColumn); // 获取指定列的投影表达式

        // 如果是直接引用输入列
        if (rexNode instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) rexNode; // 转换为直接引用
            return mq.getColumnOrigins(input, inputRef.getIndex()); // 返回对应输入列的血缘信息
        }

        // 对于其他表达式，可能是从多个列派生，递归获取血缘信息
        final Set<RelColumnOrigin> set = getMultipleColumns(rexNode, input, mq);
        return createDerivedColumnOrigins(set); // 标记为派生血缘
    }

    /**
     * 获取 Filter 类型的列血缘信息。
     * Filter 仅过滤行，因此不修改列的血缘。
     *
     * @param rel          Filter 类型的关系表达式
     * @param mq           元数据查询工具
     * @param iOutputColumn 输出列的索引
     * @return 输出列的血缘信息集合
     */
    public Set<RelColumnOrigin> getColumnOrigins(Filter rel, RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn); // 直接返回输入列的血缘信息
    }

    /**
     * 获取 Sort 类型的列血缘信息。
     * Sort 仅排序行，因此不修改列的血缘。
     *
     * @param rel          Sort 类型的关系表达式
     * @param mq           元数据查询工具
     * @param iOutputColumn 输出列的索引
     * @return 输出列的血缘信息集合
     */
    public Set<RelColumnOrigin> getColumnOrigins(Sort rel, RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn); // 直接返回输入列的血缘信息
    }

    /**
     * 获取 TableModify 类型的列血缘信息。
     * TableModify 表示对表的修改操作（如插入、更新、删除），但其输出列的血缘与输入列相同。
     *
     * @param rel          TableModify 类型的关系表达式
     * @param mq           元数据查询工具
     * @param iOutputColumn 输出列的索引
     * @return 输出列的血缘信息集合
     */
    public Set<RelColumnOrigin> getColumnOrigins(TableModify rel, RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn); // 直接返回输入列的血缘信息
    }

    /**
     * 获取 Exchange 类型的列血缘信息。
     * Exchange 仅调整数据分布，不修改列的血缘。
     *
     * @param rel          Exchange 类型的关系表达式
     * @param mq           元数据查询工具
     * @param iOutputColumn 输出列的索引
     * @return 输出列的血缘信息集合
     */
    public Set<RelColumnOrigin> getColumnOrigins(Exchange rel, RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn); // 直接返回输入列的血缘信息
    }

    /**
     * 获取 TableFunctionScan 类型的列血缘信息。
     * TableFunctionScan 表示表值函数（TVF），支持通过列映射或直接引用追踪血缘。
     *
     * @param rel          TableFunctionScan 类型的关系表达式
     * @param mq           元数据查询工具
     * @param iOutputColumn 输出列的索引
     * @return 输出列的血缘信息集合
     */
    public Set<RelColumnOrigin> getColumnOrigins(TableFunctionScan rel, RelMetadataQuery mq, int iOutputColumn) {
        Set<RelColumnOrigin> set = new LinkedHashSet<>();
        Set<RelColumnMapping> mappings = rel.getColumnMappings(); // 获取列映射关系

        if (mappings == null) {
            // 如果没有列映射，则尝试直接从输入追踪血缘
            if (!rel.getInputs().isEmpty()) {
                RelNode input = rel.getInput(0); // 获取输入节点
                int nInputColumns = input.getRowType().getFieldList().size();

                if (iOutputColumn < nInputColumns) {
                    return mq.getColumnOrigins(input, iOutputColumn); // 返回输入列的血缘
                } else {
                    // 处理表函数的特殊列
                    RexCall rexCall = (RexCall) rel.getCall();
                    List<RexNode> operands = rexCall.getOperands();
                    // TODO 为什么要 operands.get(1) ？？？
                    RexInputRef rexInputRef = (RexInputRef) ((RexCall) operands.get(1)).getOperands().get(0);
                    set = mq.getColumnOrigins(input, rexInputRef.getIndex());

                    String transform = rexCall.op.getName()
                            + DELIMITER
                            + rexCall.getType().getFieldNames().get(iOutputColumn);
                    return createDerivedColumnOrigins(set, transform); // 创建派生血缘信息
                }
            } else {
                return set; // 无输入节点，返回空集合
            }
        }

        // 使用列映射关系追踪血缘
        for (RelColumnMapping mapping : mappings) {
            if (mapping.iOutputColumn != iOutputColumn) {
                continue; // 跳过非目标列的映射
            }

            final RelNode input = rel.getInputs().get(mapping.iInputRel);
            final int column = mapping.iInputColumn;
            Set<RelColumnOrigin> origins = mq.getColumnOrigins(input, column);

            if (origins == null) {
                return Collections.emptySet(); // 如果血缘为空，返回空集合
            }

            if (mapping.derived) {
                origins = createDerivedColumnOrigins(origins); // 如果是派生列，标记派生信息
            }

            set.addAll(origins); // 添加到结果集合
        }
        return set;
    }


    /**
     * 通用规则，当其他具体规则不适用时使用。
     */
    @SuppressWarnings("squid:S1172")
    public Set<RelColumnOrigin> getColumnOrigins(RelNode rel, RelMetadataQuery mq, int iOutputColumn) {
        // NOTE jvs 28-Mar-2006:
        // 对于支持投影的物理表表达式可能不适用此逻辑。在这种情况下，需要插件开发者提供具体的实现。

        if (!rel.getInputs().isEmpty()) {
            // 如果当前节点有输入（非叶子节点），无法直接推断列血缘，返回空集合。
            return Collections.emptySet();
        }

        final Set<RelColumnOrigin> set = new LinkedHashSet<>();

        RelOptTable table = rel.getTable();
        if (table == null) {
            // 如果当前节点没有绑定表（例如 VALUES 子句生成的值），则无法推断列血缘，返回空集合。
            return set;
        }

        // 检测是否有投影的物理表表达式，无法明确列对应关系时返回空集合。
        if (table.getRowType() != rel.getRowType()) {
            return Collections.emptySet();
        }

        // 默认情况下，假设列直接来源于表中的对应列。
        set.add(new RelColumnOrigin(table, iOutputColumn, false));
        return set;
    }

    /**
     * 创建派生列血缘信息。
     *
     * @param inputSet 输入列血缘集合
     * @return 派生列血缘集合
     */
    private Set<RelColumnOrigin> createDerivedColumnOrigins(Set<RelColumnOrigin> inputSet) {
        if (inputSet == null) {
            return Collections.emptySet();
        }

        final Set<RelColumnOrigin> set = new LinkedHashSet<>();
        for (RelColumnOrigin rco : inputSet) {
            // 创建派生列血缘，标记为派生列。
            RelColumnOrigin derived = new RelColumnOrigin(
                    rco.getOriginTable(),
                    rco.getOriginColumnOrdinal(),
                    true // 标记为派生列
            );
            set.add(derived);
        }
        return set;
    }

    /**
     * 创建带转换逻辑的派生列血缘信息。
     *
     * @param inputSet  输入列血缘集合
     * @param transform 转换表达式
     * @return 派生列血缘集合
     */
    private Set<RelColumnOrigin> createDerivedColumnOrigins(Set<RelColumnOrigin> inputSet, Object transform) {
        if (inputSet == null || inputSet.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<RelColumnOrigin> set = new LinkedHashSet<>();
        String finalTransform = computeTransform(inputSet, transform); // 计算转换表达式

        for (RelColumnOrigin rco : inputSet) {
            // 创建包含转换逻辑的派生列血缘
            RelColumnOrigin derived = new RelColumnOrigin(
                    rco.getOriginTable(),
                    rco.getOriginColumnOrdinal(),
                    true, // 标记为派生列
                    finalTransform // 附加转换表达式
            );
            set.add(derived);
        }
        return set;
    }

    /**
     * 替换转换表达式中以 $ 开头的变量为真实字段信息。
     *
     * @param inputSet  输入列血缘集合
     * @param transform 转换表达式
     * @return 替换后的转换表达式
     */
    private String computeTransform(Set<RelColumnOrigin> inputSet, Object transform) {
        LOG.debug("origin transform: {}, class: {}", transform, transform.getClass());
        String finalTransform = transform.toString();

        // 匹配形如 $0, $1 的变量
        Matcher matcher = pattern.matcher(finalTransform);

        Set<String> operandSet = new LinkedHashSet<>();
        while (matcher.find()) {
            operandSet.add(matcher.group());
        }

        if (operandSet.isEmpty()) {
            return finalTransform; // 如果没有变量，直接返回原始表达式
        }

        if (inputSet.size() != operandSet.size()) {
            // 如果变量数量与输入列数量不匹配，记录警告日志并返回 null
            LOG.warn("源表字段数量 [{}] 与操作数数量 [{}] 不匹配", inputSet.size(), operandSet.size());
            return null;
        }

        // 构建字段映射表
        Map<String, String> sourceColumnMap = buildSourceColumnMap(inputSet, transform);

        // 替换变量为真实字段名
        matcher = pattern.matcher(finalTransform);
        String temp;
        while (matcher.find()) {
            temp = matcher.group();
            finalTransform = finalTransform.replace(temp, sourceColumnMap.get(temp));
        }

        // 临时处理特殊字符（如 "_UTF-16LE"）
        finalTransform = finalTransform.replace("_UTF-16LE", "");
        LOG.debug("final transform: {}", finalTransform);
        return finalTransform;
    }


    /**
     * 根据输入字段集合 (inputSet) 的生成顺序，构建字段映射表。
     * <p>
     * 示例：ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $3 DESC NULLS LAST)
     * - 遍历字符串时的字段顺序为 $0, $3。
     * - 实际生成 inputSet 的顺序为 $3, $0。
     * 该方法确保字段映射表按照 inputSet 的实际顺序生成。
     */
    private Map<String, String> buildSourceColumnMap(Set<RelColumnOrigin> inputSet, Object transform) {
        Set<Object> traversalSet = new LinkedHashSet<>();

        // 如果转换表达式是 AggregateCall 类型
        if (transform instanceof AggregateCall) {
            AggregateCall call = (AggregateCall) transform;
            traversalSet.addAll(call.getArgList()); // 收集所有参数索引
        }
        // 如果转换表达式是 RexNode 类型
        else if (transform instanceof RexNode) {
            RexNode rexNode = (RexNode) transform;

            // 使用 RexVisitor 遍历表达式，提取引用字段索引
            RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
                @Override
                public Void visitInputRef(RexInputRef inputRef) {
                    traversalSet.add(inputRef.getIndex());
                    return null;
                }

                @Override
                public Void visitPatternFieldRef(RexPatternFieldRef fieldRef) {
                    traversalSet.add(fieldRef.getIndex());
                    return null;
                }

                @Override
                public Void visitFieldAccess(RexFieldAccess fieldAccess) {
                    traversalSet.add(fieldAccess.toString().replace("$", ""));
                    return null;
                }
            };
            rexNode.accept(visitor); // 遍历表达式树
        }

        // 构建字段映射表，将字段索引与其实际名称映射
        Map<String, String> sourceColumnMap = new HashMap<>(INITIAL_CAPACITY);
        Iterator<String> iterator = optimizeSourceColumnSet(inputSet).iterator();
        traversalSet.forEach(index -> sourceColumnMap.put("$" + index, iterator.next()));
        LOG.debug("sourceColumnMap: {}", sourceColumnMap);
        return sourceColumnMap;
    }

    /**
     * 优化字段显示以提高可读性。
     * 根据 catalog、database 和 table 的唯一性，决定字段显示的粒度：
     * - 如果 catalog, database 和 table 相同，仅显示字段名。
     * - 如果 catalog 和 database 相同，显示 table 和字段名。
     * - 如果 catalog 相同，显示 database, table 和字段名。
     * - 否则，显示完整路径。
     */
    private Set<String> optimizeSourceColumnSet(Set<RelColumnOrigin> inputSet) {
        Set<String> catalogSet = new HashSet<>();
        Set<String> databaseSet = new HashSet<>();
        Set<String> tableSet = new HashSet<>();
        Set<List<String>> qualifiedSet = new LinkedHashSet<>();

        for (RelColumnOrigin rco : inputSet) {
            RelOptTable originTable = rco.getOriginTable(); // 获取原始表
            List<String> qualifiedName = originTable.getQualifiedName(); // 获取完整路径

            // 添加 catalog、database、table 的唯一性信息
            catalogSet.add(qualifiedName.get(0));
            databaseSet.add(qualifiedName.get(1));
            tableSet.add(qualifiedName.get(2));

            // 获取字段名，如果有转换信息则使用转换名
            String field = rco.getTransform() != null
                    ? rco.getTransform()
                    : originTable.getRowType().getFieldNames().get(rco.getOriginColumnOrdinal());

            // 构建完整路径列表
            List<String> qualifiedList = new ArrayList<>(qualifiedName);
            qualifiedList.add(field);
            qualifiedSet.add(qualifiedList);
        }

        // 根据唯一性决定字段显示粒度
        if (catalogSet.size() == 1 && databaseSet.size() == 1 && tableSet.size() == 1) {
            return optimizeName(qualifiedSet, e -> e.get(3)); // 仅显示字段名
        } else if (catalogSet.size() == 1 && databaseSet.size() == 1) {
            return optimizeName(qualifiedSet, e -> String.join(DELIMITER, e.subList(2, 4))); // 显示表名和字段名
        } else if (catalogSet.size() == 1) {
            return optimizeName(qualifiedSet, e -> String.join(DELIMITER, e.subList(1, 4))); // 显示数据库、表名和字段名
        } else {
            return optimizeName(qualifiedSet, e -> String.join(DELIMITER, e)); // 显示完整路径
        }
    }

    /**
     * 根据映射器函数优化字段名。
     *
     * @param qualifiedSet 完整路径集合
     * @param mapper       用于优化路径显示的映射器函数
     * @return 优化后的字段名集合
     */
    private Set<String> optimizeName(Set<List<String>> qualifiedSet, Function<List<String>, String> mapper) {
        return qualifiedSet.stream()
                .map(mapper)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * 从表达式中获取多个列的血缘信息。
     *
     * @param rexNode 表达式节点
     * @param input   输入关系表达式
     * @param mq      元数据查询工具
     * @return 多个列的血缘信息集合
     */
    private Set<RelColumnOrigin> getMultipleColumns(RexNode rexNode, RelNode input, final RelMetadataQuery mq) {
        final Set<RelColumnOrigin> set = new LinkedHashSet<>();

        // 使用 RexVisitor 遍历表达式，提取输入列的血缘信息
        RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitInputRef(RexInputRef inputRef) {
                Set<RelColumnOrigin> inputSet = mq.getColumnOrigins(input, inputRef.getIndex());
                if (inputSet != null) {
                    set.addAll(inputSet); // 添加血缘信息
                }
                return null;
            }
        };
        rexNode.accept(visitor); // 遍历表达式树
        return set;
    }
}
