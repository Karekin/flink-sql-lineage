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

package com.hw.lineage.flink.parse;

import com.hw.lineage.flink.basic.AbstractBasicTest;

import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.ValidationException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertThrows;

/**
 * @description: ParseTest类，用于测试SQL语法解析和验证的功能。
 * 包括正确SQL的解析、语法错误、表错误、字段错误等不同场景下的测试。
 * 主要用于验证SQL解析器和验证器的功能完整性和异常处理能力。
 * 测试框架：JUnit。
 * @author: HamaWhite
 */
public class ParseTest extends AbstractBasicTest {

    /**
     * 在每个测试方法执行前调用，用于创建测试所需的表。
     */
    @Before
    public void createTable() {
        // 创建MySQL CDC表 ods_mysql_users
        createTableOfOdsMysqlUsers();
    }

    /**
     * 测试场景：验证一条语法正确的SQL。
     * <p>
     * SQL功能：查询表 ods_mysql_users 的字段 id、name 和 birthday。
     */
    @Test
    public void testParse() {
        context.parseValidate("SELECT id, name, birthday FROM ods_mysql_users");
    }

    /**
     * 测试场景：验证一条带有语法错误的SQL。
     * <p>
     * SQL功能：错误的关键字（ERROR_FROM），导致解析失败。
     * <p>
     * 期望结果：抛出 SqlParserException 异常，并提示具体错误信息。
     */
    @Test
    public void testParseWithErrorSyntax() {
        assertThrows(
                "SQL parse failed. Encountered \"ods_mysql_users\" at line 1, column 38.",
                SqlParserException.class,
                () -> context.parseValidate("SELECT id, name, birthday ERROR_FROM ods_mysql_users"));
    }

    /**
     * 测试场景：验证一条使用了不存在的表的SQL。
     * <p>
     * SQL功能：查询表 error_ods_mysql_users（实际不存在），导致验证失败。
     * <p>
     * 期望结果：抛出 ValidationException 异常，并提示具体错误信息。
     */
    @Test
    public void testParseWithErrorTable() {
        assertThrows(
                "SQL validation failed. From line 1, column 32 to line 1, column 52: Object 'error_ods_mysql_users' not found",
                ValidationException.class,
                () -> context.parseValidate("SELECT id, name, birthday FROM error_ods_mysql_users"));
    }

    /**
     * 测试场景：验证一条使用了不存在的字段的SQL。
     * <p>
     * SQL功能：查询表 ods_mysql_users 中的字段 error_id（实际不存在），导致验证失败。
     * <p>
     * 期望结果：抛出 ValidationException 异常，并提示具体错误信息。
     */
    @Test
    public void testParseWithErrorFiled() {
        assertThrows(
                "SQL validation failed. From line 1, column 8 to line 1, column 15: Column 'error_id' not found in any table",
                ValidationException.class,
                () -> context.parseValidate("SELECT error_id, name, birthday FROM ods_mysql_users"));
    }

    /**
     * 测试场景：验证一条SHOW命令的SQL。
     * <p>
     * SQL功能：展示当前Catalog列表。
     */
    @Test
    public void testParseShow() {
        context.parseValidate("SHOW CATALOGS");
    }
}

