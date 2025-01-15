package com.hw.lineage.flink.tablefuncion;

import com.hw.lineage.flink.basic.AbstractBasicTest;

import org.junit.Before;
import org.junit.Test;

/**
 * @description: TableFunctionTest 类，用于测试在 Flink 中使用表函数（UDTF）的 SQL 数据血缘分析功能。
 * 表函数（UDTF）是一种可以将一行输入扩展为多行输出的函数，适用于对字段进行拆分、扩展等操作。
 * 测试用例主要涵盖了以下场景：
 * - 表函数的基本使用。
 * - 表函数与左连接的结合使用。
 * - 表函数输出字段的重命名。
 * - 表函数中包含其他函数的处理。
 * 数据来源包括 MySQL CDC 表，数据写入 Hudi 表。
 * <p>
 * 来源链接：<a href="https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/functions/udfs/#table-functions">Flink Table Functions</a>
 * </p>
 */
public class TableFunctionTest extends AbstractBasicTest {

    /**
     * 在每个测试用例执行之前，创建测试所需的表和函数。
     */
    @Before
    public void createTable() {
        // 创建 MySQL CDC 表 ods_mysql_users
        createTableOfOdsMysqlUsers();

        // 创建表函数 my_split_udtf
        createFunctionOfMySplit();

        // 创建 Hudi Sink 表 dwd_hudi_users
        createTableOfDwdHudiUsers();
    }

    /**
     * 测试场景：表函数的基本使用。
     * <p>
     * SQL 功能：从 MySQL CDC 表中使用表函数 my_split_udtf 拆分字段，并将结果插入到 Hudi 表中。
     */
    @Test
    public void testInsertSelectWithUDTF() {
        String sql = "INSERT INTO dwd_hudi_users " +
                "SELECT " +
                "   length ," +
                "   name ," +
                "   word as company_name ," +
                "   birthday ," +
                "   ts ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM" +
                "   ods_mysql_users ," +
                "   LATERAL TABLE(my_split_udtf(name))";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"ods_mysql_users", "name", "dwd_hudi_users", "id", "my_split_udtf(name).length"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "name"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "company_name", "my_split_udtf(name).word"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "birthday"},
                {"ods_mysql_users", "ts", "dwd_hudi_users", "ts"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "partition", "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        // 分析血缘关系和表函数的使用
        analyzeLineage(sql, expectedArray);
        analyzeFunction(sql, new String[]{"my_split_udtf"});
    }

    /**
     * 测试场景：表函数与左连接的结合使用。
     * <p>
     * SQL 功能：将表函数的结果与源表通过左连接关联，并插入到 Hudi 表中。
     */
    @Test
    public void testInsertSelectLeftJoinUDTF() {
        String sql = "INSERT INTO dwd_hudi_users " +
                "SELECT " +
                "   length ," +
                "   name ," +
                "   word as company_name ," +
                "   birthday ," +
                "   ts ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM" +
                "   ods_mysql_users " +
                "LEFT JOIN " +
                "   LATERAL TABLE(my_split_udtf(name)) ON TRUE";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"ods_mysql_users", "name", "dwd_hudi_users", "id", "my_split_udtf(name).length"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "name"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "company_name", "my_split_udtf(name).word"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "birthday"},
                {"ods_mysql_users", "ts", "dwd_hudi_users", "ts"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "partition", "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        // 分析血缘关系和表函数的使用
        analyzeLineage(sql, expectedArray);
        analyzeFunction(sql, new String[]{"my_split_udtf"});
    }

    /**
     * 测试场景：表函数输出字段重命名。
     * <p>
     * SQL 功能：在 SQL 中对表函数的输出字段重命名，并将结果插入到 Hudi 表中。
     */
    @Test
    public void testInsertSelectLeftJoinAndRenameUDTF() {
        String sql = "INSERT INTO dwd_hudi_users " +
                "SELECT " +
                "   new_length ," +
                "   name ," +
                "   new_word as company_name ," +
                "   birthday ," +
                "   ts ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM" +
                "   ods_mysql_users " +
                "LEFT JOIN " +
                "   LATERAL TABLE(my_split_udtf(name)) AS T(new_word, new_length) ON TRUE";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"ods_mysql_users", "name", "dwd_hudi_users", "id", "my_split_udtf(name).length"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "name"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "company_name", "my_split_udtf(name).word"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "birthday"},
                {"ods_mysql_users", "ts", "dwd_hudi_users", "ts"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "partition", "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        // 分析血缘关系和表函数的使用
        analyzeLineage(sql, expectedArray);
        analyzeFunction(sql, new String[]{"my_split_udtf"});
    }

    /**
     * 测试场景：表函数中使用其他函数（如 CAST）。
     * <p>
     * SQL 功能：对字段进行类型转换后传入表函数，并将结果插入到 Hudi 表中。
     */
    @Test
    public void testInsertSelectWithFunctionInUDTF() {
        String sql = "INSERT INTO dwd_hudi_users " +
                "SELECT " +
                "   new_length ," +
                "   name ," +
                "   new_word as company_name ," +
                "   birthday ," +
                "   ts ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM" +
                "   ods_mysql_users ," +
                "   LATERAL TABLE(my_split_udtf(CAST(name AS STRING))) AS T(new_word, new_length)";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"ods_mysql_users", "name", "dwd_hudi_users", "id",
                        "my_split_udtf(CAST(name):VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\").length"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "name"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "company_name",
                        "my_split_udtf(CAST(name):VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\").word"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "birthday"},
                {"ods_mysql_users", "ts", "dwd_hudi_users", "ts"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "partition", "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        // 分析血缘关系和表函数的使用
        analyzeLineage(sql, expectedArray);
        analyzeFunction(sql, new String[]{"my_split_udtf"});
    }

    /**
     * 创建表函数 my_split_udtf。
     */
    private void createFunctionOfMySplit() {
        context.execute("DROP FUNCTION IF EXISTS my_split_udtf");

        context.execute("CREATE FUNCTION IF NOT EXISTS my_split_udtf " +
                "AS 'com.hw.lineage.flink.tablefuncion.MySplitFunction'");
    }
}

