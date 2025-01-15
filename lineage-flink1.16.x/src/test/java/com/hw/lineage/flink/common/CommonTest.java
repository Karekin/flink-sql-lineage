package com.hw.lineage.flink.common;

import com.hw.lineage.flink.basic.AbstractBasicTest;

import org.apache.flink.table.api.ValidationException;
import org.junit.Before;
import org.junit.Test;

/**
 * @description: CommonTest类，用于测试数据血缘分析和插入选择操作
 */
public class CommonTest extends AbstractBasicTest {

    /**
     * 在每个测试方法执行前调用，创建测试所需的表和函数。
     */
    @Before
    public void createTable() {
        // 创建 MySQL CDC 表 ods_mysql_users
        createTableOfOdsMysqlUsers();

        // 创建 MySQL 维表 dim_mysql_company
        createTableOfDimMysqlCompany();

        // 创建 Hudi Sink 表 dwd_hudi_users
        createTableOfDwdHudiUsers();

        // 创建自定义函数 my_suffix_udf
        createFunctionOfMySuffix();
    }

    /**
     * 测试插入选择操作中查询字段和目标字段不匹配的情况。
     * <p>
     * 测试场景：从 MySQL CDC 表插入数据到 Hudi 表，但查询字段与目标字段存在不匹配。
     * <p>
     * 期望结果：抛出 ValidationException 异常。
     */
    @Test(expected = ValidationException.class)
    public void testInsertSelectMismatchField() {
        String sql = "INSERT INTO dwd_hudi_users " +
                "SELECT " +
                "   id ," +
                "   name ," +
                "   birthday ," +
                "   ts ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM" +
                "   ods_mysql_users";

        // 分析数据血缘并触发异常验证
        context.analyzeLineage(sql);
    }

    /**
     * 测试插入选择操作。
     * <p>
     * 测试场景：从 MySQL CDC 表插入数据到 Hudi 表，字段匹配且包含字段别名。
     */
    @Test
    public void testInsertSelect() {
        String sql = "INSERT INTO dwd_hudi_users " +
                "SELECT " +
                "   id ," +
                "   name ," +
                "   name as company_name ," +
                "   birthday ," +
                "   ts ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM" +
                "   ods_mysql_users";

        // 预期的血缘分析结果
        String[][] expectedArray = {
                {"ods_mysql_users", "id", "dwd_hudi_users", "id"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "name"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "company_name"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "birthday"},
                {"ods_mysql_users", "ts", "dwd_hudi_users", "ts"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "partition", "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        // 分析数据血缘
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 测试带有自定义函数的插入选择操作。
     * <p>
     * 测试场景：从 MySQL CDC 表插入数据到 Hudi 表，使用自定义函数对字段进行处理。
     */
    @Test
    public void testInsertSelectWithUDF() {
        String sql = "INSERT INTO dwd_hudi_users " +
                "SELECT " +
                "   id ," +
                "   my_suffix_udf(name) ," +
                "   name as company_name ," +
                "   birthday ," +
                "   ts ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM" +
                "   ods_mysql_users";

        // 预期的血缘分析结果
        String[][] expectedArray = {
                {"ods_mysql_users", "id", "dwd_hudi_users", "id"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "name", "my_suffix_udf(name)"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "company_name"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "birthday"},
                {"ods_mysql_users", "ts", "dwd_hudi_users", "ts"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "partition", "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        // 分析数据血缘和函数使用
        analyzeLineage(sql, expectedArray);
        analyzeFunction(sql, new String[]{"my_suffix_udf"});
    }

    /**
     * 测试带有函数嵌套的插入选择操作。
     * <p>
     * 测试场景：从 MySQL CDC 表插入数据到 Hudi 表，使用多个函数对字段进行处理。
     */
    @Test
    public void testInsertSelectWithFunctionCover() {
        String sql = "INSERT INTO dwd_hudi_users " +
                "SELECT " +
                "   id ," +
                "   LOWER(my_suffix_udf(name)) ," +
                "   UPPER(TRIM(name)) as company_name ," +
                "   birthday ," +
                "   ts ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM" +
                "   ods_mysql_users";

        // 预期的血缘分析结果
        String[][] expectedArray = {
                {"ods_mysql_users", "id", "dwd_hudi_users", "id"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "name", "LOWER(my_suffix_udf(name))"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "company_name", "UPPER(TRIM(name))"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "birthday"},
                {"ods_mysql_users", "ts", "dwd_hudi_users", "ts"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "partition", "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        // 分析数据血缘和函数使用
        analyzeLineage(sql, expectedArray);
        analyzeFunction(sql, new String[]{"my_suffix_udf"});
    }

    /**
     * 测试插入操作时指定分区。
     * <p>
     * 测试场景：从 MySQL CDC 表插入数据到 Hudi 表，并指定分区值。
     */
    @Test
    public void testInsertPartitionSelect() {
        String sql = "INSERT INTO dwd_hudi_users PARTITION (`partition`='20220824') " +
                "SELECT " +
                "   id ," +
                "   name ," +
                "   name as company_name ," +
                "   birthday ," +
                "   ts " +
                "FROM" +
                "   ods_mysql_users";

        // 预期的血缘分析结果
        String[][] expectedArray = {
                {"ods_mysql_users", "id", "dwd_hudi_users", "id"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "name"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "company_name"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "birthday"},
                {"ods_mysql_users", "ts", "dwd_hudi_users", "ts"}
        };

        // 分析数据血缘
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 测试插入操作时指定分区和列列表。
     * <p>
     * 测试场景：从 MySQL CDC 表插入数据到 Hudi 表，指定分区值并明确指定列列表。
     */
    @Test
    public void testInsertPartitionWithColumnListSelect() {
        String sql = "INSERT INTO dwd_hudi_users PARTITION (`partition`='20220824') (id,company_name) " +
                "SELECT " +
                "   id ," +
                "   name " +
                "FROM" +
                "   ods_mysql_users";

        // 预期的血缘分析结果
        String[][] expectedArray = {
                {"ods_mysql_users", "id", "dwd_hudi_users", "id"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "company_name"}
        };

        // 分析数据血缘
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 测试嵌套查询的插入操作。
     * <p>
     * 测试场景：从 MySQL CDC 表插入数据到 Hudi 表，使用嵌套查询计算聚合结果并作为插入源。
     */
    @Test
    public void testInsertSelectSelect() {
        /*
            整体逻辑：1、从 ods_mysql_users 中获取数据，按 name 和 DATE_FORMAT(birthday, 'yyyyMMdd') 分组，
            计算 id 的总和，并生成一些衍生字段。2、处理分组结果，例如对 sum_id 取绝对值。3、将加工后的数据插入到目标表 dwd_hudi_users 中。
            TODO 感觉解析结果不对，少
         */
        String sql = "INSERT INTO dwd_hudi_users " +
                "SELECT " +
                "   ABS(sum_id) ," +
                "   name ," +
                "   company_name ," +
                "   birthday1 ," +
                "   ts ," +
                "   p " +
                "FROM ( " +
                "   SELECT " +
                "       SUM(id) as sum_id ," +
                "       name," +
                "       '1' as company_name ," +
                "       NOW() as birthday1 ," +
                "       NOW() as ts ," +
                "       DATE_FORMAT(birthday, 'yyyyMMdd') as p " +
                "   FROM " +
                "       ods_mysql_users " +
                "   GROUP BY" +
                "       name, " +
                "       DATE_FORMAT(birthday, 'yyyyMMdd')" +
                ")";

        // 预期的血缘分析结果
        String[][] expectedArray = {
                {"ods_mysql_users", "id", "dwd_hudi_users", "id", "ABS(SUM(id))"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "name"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "partition", "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        // 分析数据血缘
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 测试两表 Join 的插入操作。
     * <p>
     * 测试场景：从 MySQL CDC 表和 MySQL 维表进行 Join 操作后插入数据到 Hudi 表，
     * 并使用系统函数 CONCAT 连接字段。
     */
    @Test
    public void testInsertSelectTwoJoin() {
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
                "   dim_mysql_company as b " +
                "ON " +
                "   a.id = b.user_id";

        // 预期的血缘分析结果
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

        // 分析数据血缘
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 创建自定义函数 my_suffix_udf，用于后续测试中使用。
     */
    private void createFunctionOfMySuffix() {
        // 删除已存在的函数（如果存在）
        context.execute("DROP FUNCTION IF EXISTS my_suffix_udf");

        // 创建新的自定义函数
        context.execute("CREATE FUNCTION IF NOT EXISTS my_suffix_udf " +
                "AS 'com.hw.lineage.flink.common.MySuffixFunction'");
    }

}