package com.hw.lineage.flink.watermark;

import com.hw.lineage.flink.basic.AbstractBasicTest;

import org.junit.Before;
import org.junit.Test;

/**
 * @description: WatermarkTest类，用于测试带有水印（Watermark）的表在 Flink 中的操作及数据血缘关系解析。
 * 测试覆盖以下场景：
 * - 从带有水印的 MySQL CDC 表中读取数据并写入 Hudi 表。
 * - 将带有水印的 MySQL CDC 表与维表（Dim Table）进行 Join，并写入 Hudi 表。
 * 水印用于处理事件时间数据，确保对延迟事件的处理。
 * </p>
 */
public class WatermarkTest extends AbstractBasicTest {

    /**
     * 在每个测试用例执行之前，创建测试所需的表。
     */
    @Before
    public void createTable() {
        // 创建带有水印的 MySQL CDC 表 ods_mysql_users_watermark
        createTableOfOdsMysqlUsersWatermark();

        // 创建 MySQL 维表 dim_mysql_company
        createTableOfDimMysqlCompany();

        // 创建 Hudi Sink 表 dwd_hudi_users
        createTableOfDwdHudiUsers();
    }

    /**
     * 测试场景：从带有水印的 MySQL CDC 表中读取数据并写入 Hudi 表。
     * <p>
     * SQL 功能：将 MySQL 表 ods_mysql_users_watermark 的数据处理后插入到 Hudi 表 dwd_hudi_users 中。
     */
    @Test
    public void testInsertSelectWatermark() {
        String sql = "INSERT INTO dwd_hudi_users " +
                "SELECT " +
                "   id ," +
                "   name ," +
                "   name as company_name ," +
                "   birthday ," +
                "   ts ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM" +
                "   ods_mysql_users_watermark";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"ods_mysql_users_watermark", "id", "dwd_hudi_users", "id"},
                {"ods_mysql_users_watermark", "name", "dwd_hudi_users", "name"},
                {"ods_mysql_users_watermark", "name", "dwd_hudi_users", "company_name"},
                {"ods_mysql_users_watermark", "birthday", "dwd_hudi_users", "birthday"},
                {"ods_mysql_users_watermark", "ts", "dwd_hudi_users", "ts"},
                {"ods_mysql_users_watermark", "birthday", "dwd_hudi_users", "partition",
                        "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        // 分析血缘关系
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 测试场景：从带有水印的 MySQL CDC 表与维表进行 Join 操作，并将结果写入 Hudi 表。
     * <p>
     * SQL 功能：将 MySQL 表 ods_mysql_users_watermark 与维表 dim_mysql_company 按 ID 进行关联，
     * 将关联结果插入到 Hudi 表 dwd_hudi_users 中。
     */
    @Test
    public void testInsertSelectTwoJoinWatermark() {
        String sql = "INSERT into dwd_hudi_users " +
                "SELECT " +
                "       a.id as id1," +
                "       CONCAT(a.name,b.company_name) , " +
                "       b.company_name , " +
                "       a.birthday ," +
                "       a.ts ," +
                "       DATE_FORMAT(a.birthday, 'yyyyMMdd') as p " +
                "FROM" +
                "       ods_mysql_users_watermark as a " +
                "JOIN " +
                "   dim_mysql_company as b " +
                "ON " +
                "   a.id = b.user_id";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"ods_mysql_users_watermark", "id", "dwd_hudi_users", "id"},
                {"ods_mysql_users_watermark", "name", "dwd_hudi_users", "name",
                        "CONCAT(ods_mysql_users_watermark.name, dim_mysql_company.company_name)"},
                {"dim_mysql_company", "company_name", "dwd_hudi_users", "name",
                        "CONCAT(ods_mysql_users_watermark.name, dim_mysql_company.company_name)"},
                {"dim_mysql_company", "company_name", "dwd_hudi_users", "company_name"},
                {"ods_mysql_users_watermark", "birthday", "dwd_hudi_users", "birthday"},
                {"ods_mysql_users_watermark", "ts", "dwd_hudi_users", "ts"},
                {"ods_mysql_users_watermark", "birthday", "dwd_hudi_users", "partition",
                        "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        // 分析血缘关系
        analyzeLineage(sql, expectedArray);
    }
}

