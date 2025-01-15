package com.hw.lineage.flink.window;

import com.hw.lineage.flink.basic.AbstractBasicTest;

import org.junit.Before;
import org.junit.Test;

/**
 * @description: WindowTest 类，用于测试 Flink SQL 中的窗口函数（如 ROW_NUMBER()）及其数据血缘关系的解析。
 * 测试场景包括：
 * - 使用 ROW_NUMBER() 对流式数据进行排序和分组操作。
 * - 将处理结果写入 Hudi 表。
 * 窗口函数可以基于分组和排序生成行号，用于去重、排名等操作。
 * </p>
 */
public class WindowTest extends AbstractBasicTest {

    /**
     * 在每个测试用例执行之前，创建测试所需的表。
     */
    @Before
    public void createTable() {
        // 创建 MySQL CDC 表 ods_mysql_users
        createTableOfOdsMysqlUsers();

        // 创建 Hudi Sink 表 dwd_hudi_users
        createTableOfDwdHudiUsers();
    }

    /**
     * 测试场景：使用单个 ROW_NUMBER() 窗口函数。
     * <p>
     * SQL 功能：从 MySQL 表 ods_mysql_users 中读取数据，基于 ID 分组，按时间戳（ts）降序生成行号（rowNum），
     * 将处理结果插入到 Hudi 表 dwd_hudi_users。
     */
    @Test
    public void testInsertSelectRowNumber() {
        String sql = "INSERT INTO dwd_hudi_users " +
                "SELECT " +
                "   ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) as rowNum ," +
                "   name ," +
                "   name as company_name ," +
                "   birthday ," +
                "   ts ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM" +
                "   ods_mysql_users";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"ods_mysql_users", "ts", "dwd_hudi_users", "id",
                        "ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC NULLS LAST)"},
                {"ods_mysql_users", "id", "dwd_hudi_users", "id",
                        "ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC NULLS LAST)"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "name"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "company_name"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "birthday"},
                {"ods_mysql_users", "ts", "dwd_hudi_users", "ts"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "partition", "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        // 分析血缘关系
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 测试场景：使用两个 ROW_NUMBER() 窗口函数。
     * <p>
     * SQL 功能：从 MySQL 表 ods_mysql_users 中读取数据，基于 ID 分组生成行号（rowNum），
     * 同时基于 NAME 分组生成另一个行号并将其转换为字符串类型，最终将处理结果插入到 Hudi 表 dwd_hudi_users。
     */
    @Test
    public void testInsertSelectTwoRowNumber() {
        String sql = "INSERT INTO dwd_hudi_users " +
                "SELECT " +
                "   ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) as rowNum ," +
                "   name ," +
                "   CAST(ROW_NUMBER() OVER (PARTITION BY name ORDER BY ts DESC) as STRING) as company_name ," +
                "   birthday ," +
                "   ts ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM" +
                "   ods_mysql_users";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"ods_mysql_users", "ts", "dwd_hudi_users", "id",
                        "ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC NULLS LAST)"},
                {"ods_mysql_users", "id", "dwd_hudi_users", "id",
                        "ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC NULLS LAST)"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "name"},
                {"ods_mysql_users", "ts", "dwd_hudi_users", "company_name",
                        "CAST(ROW_NUMBER() OVER (PARTITION BY name ORDER BY ts DESC NULLS LAST)):VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\" NOT NULL"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "company_name",
                        "CAST(ROW_NUMBER() OVER (PARTITION BY name ORDER BY ts DESC NULLS LAST)):VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\" NOT NULL"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "birthday"},
                {"ods_mysql_users", "ts", "dwd_hudi_users", "ts"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "partition", "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        // 分析血缘关系
        analyzeLineage(sql, expectedArray);
    }
}
