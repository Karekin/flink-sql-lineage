package com.hw.lineage.flink.tablefuncion;

import com.hw.lineage.flink.basic.AbstractBasicTest;

import org.junit.Before;
import org.junit.Test;

/**
 * @description: TableFunctionMultiParamsTest 类，用于测试带有多个参数的表函数（UDTF）的使用场景。
 * 测试的主要目的是验证 SQL 解析器在处理包含多个参数的表函数时的正确性，以及数据血缘关系的解析结果。
 * 来源链接：<a href="https://github.com/HamaWhiteGG/flink-sql-lineage/pull/73">fix parse error when table function has multiple params</a>
 * <p>
 * 测试覆盖场景：
 * - 表函数接受多个参数时的血缘关系解析。
 * - 从 MySQL CDC 表读取数据，使用表函数处理后，将结果插入到 Hudi 表中。
 * </p>
 */
public class TableFunctionMultiParamsTest extends AbstractBasicTest {

    /**
     * 在每个测试用例执行之前，创建测试所需的表和函数。
     */
    @Before
    public void createTable() {
        // 创建 MySQL CDC 表 ods_mysql_users_extra
        createTableOfOdsMysqlUsersExtra();

        // 创建表函数 split_multi_params_udtf
        createFunctionOfSplitMultiParams();

        // 创建 Hudi Sink 表 dwd_hudi_users_extra
        createTableOfDwdHudiUsersExtra();
    }

    /**
     * 测试场景：使用带有多个参数的表函数 split_multi_params_udtf。
     * <p>
     * SQL 功能：从 MySQL CDC 表中读取数据，使用表函数处理后，将结果插入到 Hudi Sink 表中。
     * 验证点：表函数的参数处理正确，输出字段的血缘关系清晰。
     */
    @Test
    public void testInsertSelectWithUDTF() {
        String sql = "INSERT INTO dwd_hudi_users_extra " +
                "SELECT " +
                "   length ," +
                "   name ," +
                "   extra_name ," +
                "   word as company_name ," +
                "   birthday ," +
                "   ts ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM" +
                "   ods_mysql_users_extra ," +
                "   LATERAL TABLE(split_multi_params_udtf(name, extra_name))";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"ods_mysql_users_extra", "name", "dwd_hudi_users_extra", "id",
                        "split_multi_params_udtf(name, extra_name).length"},
                {"ods_mysql_users_extra", "extra_name", "dwd_hudi_users_extra", "id",
                        "split_multi_params_udtf(name, extra_name).length"},
                {"ods_mysql_users_extra", "name", "dwd_hudi_users_extra", "name"},
                {"ods_mysql_users_extra", "extra_name", "dwd_hudi_users_extra", "extra_name"},
                {"ods_mysql_users_extra", "name", "dwd_hudi_users_extra", "company_name",
                        "split_multi_params_udtf(name, extra_name).word"},
                {"ods_mysql_users_extra", "extra_name", "dwd_hudi_users_extra", "company_name",
                        "split_multi_params_udtf(name, extra_name).word"},
                {"ods_mysql_users_extra", "birthday", "dwd_hudi_users_extra", "birthday"},
                {"ods_mysql_users_extra", "ts", "dwd_hudi_users_extra", "ts"},
                {"ods_mysql_users_extra", "birthday", "dwd_hudi_users_extra", "partition",
                        "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        // 分析血缘关系和表函数的使用
        analyzeLineage(sql, expectedArray);
        analyzeFunction(sql, new String[]{"split_multi_params_udtf"});
    }

    /**
     * 创建 MySQL CDC 表 ods_mysql_users_extra。
     * <p>
     * 表结构：
     * - id：主键，BIGINT 类型。
     * - name：用户姓名，STRING 类型。
     * - extra_name：额外信息，STRING 类型。
     * - birthday：生日，TIMESTAMP(3) 类型。
     * - ts：时间戳，TIMESTAMP(3) 类型。
     * - proc_time：处理时间字段，AS PROCTIME() 自动生成。
     * <p>
     * 表配置：
     * - 使用 MySQL CDC 作为数据源。
     * - 配置连接到 MySQL 数据库 demo 的 users 表。
     */
    private void createTableOfOdsMysqlUsersExtra() {
        context.execute("DROP TABLE IF EXISTS ods_mysql_users_extra ");

        context.execute("CREATE TABLE IF NOT EXISTS ods_mysql_users_extra (" +
                "       id                  BIGINT PRIMARY KEY NOT ENFORCED ," +
                "       name                STRING                          ," +
                "       extra_name          STRING                          ," +
                "       birthday            TIMESTAMP(3)                    ," +
                "       ts                  TIMESTAMP(3)                    ," +
                "       proc_time as proctime()                              " +
                ") WITH ( " +
                "       'connector' = 'mysql-cdc'            ," +
                "       'hostname'  = '127.0.0.1'       ," +
                "       'port'      = '3306'                 ," +
                "       'username'  = 'root'                 ," +
                "       'password'  = 'xxx'          ," +
                "       'server-time-zone' = 'Asia/Shanghai' ," +
                "       'database-name' = 'demo'             ," +
                "       'table-name'    = 'users' " +
                ")");
    }

    /**
     * 创建 Hudi Sink 表 dwd_hudi_users_extra。
     * <p>
     * 表结构：
     * - id：主键，BIGINT 类型。
     * - name：用户姓名，STRING 类型。
     * - extra_name：额外信息，STRING 类型。
     * - company_name：公司名称，STRING 类型。
     * - birthday：生日，TIMESTAMP(3) 类型。
     * - ts：时间戳，TIMESTAMP(3) 类型。
     * - partition：分区字段，VARCHAR 类型。
     * <p>
     * 表配置：
     * - 当前使用打印（print）作为连接器。
     */
    private void createTableOfDwdHudiUsersExtra() {
        context.execute("DROP TABLE IF EXISTS dwd_hudi_users_extra");

        context.execute("CREATE TABLE IF NOT EXISTS  dwd_hudi_users_extra ( " +
                "       id                  BIGINT PRIMARY KEY NOT ENFORCED ," +
                "       name                STRING                          ," +
                "       extra_name          STRING                          ," +
                "       company_name        STRING                          ," +
                "       birthday            TIMESTAMP(3)                    ," +
                "       ts                  TIMESTAMP(3)                    ," +
                "        `partition`        VARCHAR(20)                      " +
                ") PARTITIONED BY (`partition`) WITH ( " +
                "       'connector' = 'print'                                " +
                ")");
    }

    /**
     * 创建表函数 split_multi_params_udtf。
     * <p>
     * 功能：接受多个输入参数（如 name 和 extra_name），并将其拆分为多个输出字段（如 word 和 length）。
     */
    private void createFunctionOfSplitMultiParams() {
        context.execute("DROP FUNCTION IF EXISTS split_multi_params_udtf");

        context.execute("CREATE FUNCTION IF NOT EXISTS split_multi_params_udtf " +
                "AS 'com.hw.lineage.flink.tablefuncion.SplitMultiParamsFunction'");
    }
}

