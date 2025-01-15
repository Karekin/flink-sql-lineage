package com.hw.lineage.flink.common;

import com.hw.lineage.flink.basic.AbstractBasicTest;

import org.junit.Before;
import org.junit.Test;

/**
 * 测试用例来源：<a href="https://github.com/HamaWhiteGG/flink-sql-lineage/issues/53">main branch test error</a>，感谢贡献者。
 *
 * @description: SimpleTest类，测试SQL数据血缘分析功能，主要验证字段映射和字段拼接的正确性。
 */
public class SimpleTest extends AbstractBasicTest {

    /**
     * 在每个测试用例执行之前调用，创建测试所需的表结构。
     */
    @Before
    public void createTable() {
        // 创建源表 ods_user，模拟 MySQL 数据表
        createTableOfOdsUser();

        // 创建目标表 mysql_user，模拟 MySQL 数据表
        createTableOfMysqlUser();
    }

    /**
     * 测试字段拼接的血缘分析。
     * <p>
     * 测试场景：从源表 ods_user 插入数据到目标表 mysql_user，使用 CONCAT 函数拼接 first_name 和 last_name 生成 full_name 字段。
     */
    @Test
    public void testConcat() {
        String sql = "INSERT INTO mysql_user                            " +
                "SELECT                                                 " +
                "       id                                             ," +
                "       birthday                                       ," +
                "       CONCAT(first_name, last_name) as full_name      " +
                "FROM                                                   " +
                "       ods_user                                        ";

        // 预期的字段血缘关系
        String[][] expectedArray = {
                {"ods_user", "id", "mysql_user", "id"},
                {"ods_user", "birthday", "mysql_user", "birthday"},
                {"ods_user", "first_name", "mysql_user", "full_name", "CONCAT(first_name, last_name)"},
                {"ods_user", "last_name", "mysql_user", "full_name", "CONCAT(first_name, last_name)"}
        };

        // 分析字段血缘关系并验证
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 创建目标表 mysql_user。
     * <p>
     * 表结构：
     * - id：主键，BIGINT 类型
     * - birthday：时间戳字段，TIMESTAMP(3) 类型
     * - full_name：拼接后的全名，STRING 类型
     * <p>
     * 表连接器配置：
     * - 使用 JDBC 连接器连接到 MySQL 数据库
     * - 数据库地址：`jdbc:mysql://127.0.0.1:3306/demo`
     * - 表名：`mysql_user`
     */
    protected void createTableOfMysqlUser() {
        context.execute("DROP TABLE IF EXISTS mysql_user ");
        context.execute("CREATE TABLE IF NOT EXISTS mysql_user (    " +
                "       id                        BIGINT                    ," +
                "       birthday                  TIMESTAMP(3)              ," +
                "       full_name                 STRING                     " +
                ") WITH (                                                    " +
                "       'connector' = 'jdbc'                                ," +
                "       'url'       = 'jdbc:mysql://127.0.0.1:3306/demo'    ," +
                "       'username'  = 'root'                                ," +
                "       'password'  = 'xxx'                                 ," +
                "       'table-name'= 'mysql_user'                           " +
                ")");
    }

    /**
     * 创建源表 ods_user。
     * <p>
     * 表结构：
     * - id：主键，BIGINT 类型
     * - birthday：时间戳字段，TIMESTAMP(3) 类型
     * - first_name：名，STRING 类型
     * - last_name：姓，STRING 类型
     * - company_name：公司名称，STRING 类型
     * <p>
     * 表连接器配置：
     * - 使用 JDBC 连接器连接到 MySQL 数据库
     * - 数据库地址：`jdbc:mysql://127.0.0.1:3306/demo`
     * - 表名：`ods_user`
     */
    protected void createTableOfOdsUser() {
        context.execute("DROP TABLE IF EXISTS ods_user ");
        context.execute("CREATE TABLE IF NOT EXISTS ods_user (      " +
                "       id                        BIGINT                    ," +
                "       birthday                  TIMESTAMP(3)              ," +
                "       first_name                STRING                    ," +
                "       last_name                 STRING                    ," +
                "       company_name              STRING                     " +
                ") WITH (                                                    " +
                "       'connector' = 'jdbc'                                ," +
                "       'url'       = 'jdbc:mysql://127.0.0.1:3306/demo'    ," +
                "       'username'  = 'root'                                ," +
                "       'password'  = 'xxx'                                 ," +
                "       'table-name'= 'ods_user'                             " +
                ")");
    }
}

