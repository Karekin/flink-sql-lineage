package com.hw.lineage.flink.lookup.join;

import com.hw.lineage.flink.basic.AbstractBasicTest;

import org.junit.Before;
import org.junit.Test;

/**
 * @description: LookupJoinTest 类，用于测试基于 Lookup Join 的 SQL 血缘分析和数据处理功能。
 * 此类包含对表数据的创建和插入选择操作的测试。
 */
public class LookupJoinTest extends AbstractBasicTest {

    /**
     * 在每个测试用例执行之前调用，创建测试所需的表结构。
     */
    @Before
    public void createTable() {
        // 创建 MySQL CDC 表 ods_mysql_users，用于存储源数据
        createTableOfOdsMysqlUsers();

        // 创建 MySQL 维表 dim_mysql_company，用于 Lookup Join 操作
        createTableOfDimMysqlCompany();

        // 创建 Hudi Sink 表 dwd_hudi_users，用于存储处理后的数据
        createTableOfDwdHudiUsers();

        // 创建 Hudi Sink 表 dws_users_cnt，用于存储聚合后的结果
        createTableOfDwsHudiUsersCnt();
    }

    /**
     * 测试基于 Lookup Join 的插入选择操作。
     * <p>
     * 测试场景：从 MySQL CDC 表 ods_mysql_users 和 MySQL 维表 dim_mysql_company 进行 Lookup Join，
     * 并将处理后的数据插入到 Hudi Sink 表 dwd_hudi_users 中。
     * 其中使用了系统函数 CONCAT 对字段进行拼接处理。
     */
    @Test
    public void testInsertSelectTwoLookupJoin() {
        String sql = "INSERT into dwd_hudi_users " +
                "SELECT " +
                "       a.id as id1," +
                "       CONCAT(a.name,b.company_name) , " +
                "       b.company_name , " +
                "       a.birthday ," +
                "       a.ts ," +
                "       DATE_FORMAT(a.birthday, 'yyyyMMdd') as p " +
                "FROM" +
                "       ods_mysql_users as a " +
                "JOIN " +
                "   dim_mysql_company FOR SYSTEM_TIME AS OF a.proc_time AS b " +
                "ON " +
                "   a.id = b.user_id";

        // 预期的字段血缘关系
        String[][] expectedArray = {
                {"ods_mysql_users", "id", "dwd_hudi_users", "id"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "name",
                        "CONCAT(ods_mysql_users.name, dim_mysql_company.company_name)"},
                {"dim_mysql_company", "company_name", "dwd_hudi_users", "name",
                        "CONCAT(ods_mysql_users.name, dim_mysql_company.company_name)"},
                {"dim_mysql_company", "company_name", "dwd_hudi_users", "company_name"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "birthday"},
                {"ods_mysql_users", "ts", "dwd_hudi_users", "ts"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "partition", "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        // 分析字段血缘关系并验证
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 测试从 Hudi 表读取数据并进行聚合操作。
     * <p>
     * 测试场景：从 Hudi 表 dwd_hudi_users 中读取数据，按 id 聚合后插入到 Hudi 表 dws_users_cnt 中。
     * 使用了 COUNT(DISTINCT) 函数对 name 和 company_name 字段进行去重计数。
     */
    @Test
    public void testInsertSelectFromHudi() {
        String sql = "INSERT into dws_users_cnt " +
                "SELECT " +
                "       id," +
                "       COUNT(DISTINCT name), " +
                "       COUNT(DISTINCT company_name) " +
                "FROM" +
                "       dwd_hudi_users " +
                "GROUP BY " +
                "       id";

        // 预期的字段血缘关系
        String[][] expectedArray = {
                {"dwd_hudi_users", "id", "dws_users_cnt", "id"},
                {"dwd_hudi_users", "name", "dws_users_cnt", "name_cnt", "COUNT(DISTINCT name)"},
                {"dwd_hudi_users", "company_name", "dws_users_cnt", "company_name_cnt", "COUNT(DISTINCT company_name)"}
        };

        // 分析字段血缘关系并验证
        analyzeLineage(sql, expectedArray);
    }
}

