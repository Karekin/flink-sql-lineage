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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 测试类 RelMdColumnOriginsTest，测试方法主要用于验证 SQL 表达式中的占位符（如 $1、$5 等）
 * 能否被正确替换为对应的源列名。
 */
public class RelMdColumnOriginsTest extends TestCase {

    // 日志记录器，用于输出调试信息
    private static final Logger LOG = LoggerFactory.getLogger(RelMdColumnOriginsTest.class);

    /**
     * 测试方法 testReplaceSourceColumn。
     * 测试替换 SQL 表达式中的占位符为源列名的逻辑是否正确。
     */
    public void testReplaceSourceColumn() {
        // 定义一个源列集合，包含列名 "name" 和 "age"
        Set<String> sourceColumnSet = new LinkedHashSet<>();
        sourceColumnSet.add("name");
        sourceColumnSet.add("age");

        // 定义输入的 SQL 表达式，包含需要替换的占位符
        String input = "CONCAT($1, $1, $5)";

        // 正则表达式模式，用于匹配占位符 $1, $5 等
        Pattern pattern = Pattern.compile("\\$\\d+");
        Matcher matcher = pattern.matcher(input);

        // 用于存储在表达式中匹配到的占位符
        Set<String> operandSet = new LinkedHashSet<>();
        while (matcher.find()) {
            operandSet.add(matcher.group()); // 将匹配到的占位符添加到集合中
        }

        // 定义一个 Map，用于将占位符映射到源列名
        Map<String, String> sourceColumnMap = new HashMap<>();
        Iterator<String> iterator = sourceColumnSet.iterator();

        // 遍历所有占位符，并为每个占位符分配一个源列名
        operandSet.forEach(e -> sourceColumnMap.put(e, iterator.next()));
        LOG.debug("sourceColumnMap: {}", sourceColumnMap); // 输出映射关系到日志

        // 使用正则表达式再次匹配占位符，并替换为对应的源列名
        matcher = pattern.matcher(input);
        String temp; // 临时存储匹配到的占位符
        while (matcher.find()) {
            temp = matcher.group();
            input = input.replace(temp, sourceColumnMap.get(temp)); // 替换占位符为源列名
        }

        // 验证最终的替换结果是否与预期一致
        assertEquals("CONCAT(name, name, age)", input);
    }
}
