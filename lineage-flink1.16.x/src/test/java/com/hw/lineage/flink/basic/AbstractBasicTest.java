package com.hw.lineage.flink.basic;

import com.hw.lineage.common.model.FunctionResult;
import com.hw.lineage.common.model.LineageResult;
import com.hw.lineage.flink.LineageServiceImpl;

import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * @description: 抽象基础测试类，提供 HiveCatalog 的初始化、资源清理，以及用于创建和测试表的工具方法。
 */
public abstract class AbstractBasicTest {

    // 日志记录器，用于输出调试和信息日志
    private static final Logger LOG = LoggerFactory.getLogger(AbstractBasicTest.class);

    // Hive Catalog 的名称
    private static final String catalogName = "hive";

    // Hive 的版本
    private static final String hiveVersion = "3.1.2";

    // 默认数据库名称
    private static final String defaultDatabase = "default";

    // HiveCatalog 对象，用于与 Hive 数据源交互
    private static HiveCatalog hiveCatalog;

    // Lineage 服务实例，用于分析血缘和功能
    protected static LineageServiceImpl context;

    /**
     * 在所有测试开始前执行的初始化方法。
     * 创建 HiveCatalog 对象并打开连接，初始化 LineageServiceImpl 实例。
     */
    @BeforeClass
    public static void setup() throws Exception {
        hiveCatalog = HiveTestUtils.createHiveCatalog(catalogName, defaultDatabase, hiveVersion);
        hiveCatalog.open(); // 打开 HiveCatalog 连接

        context = new LineageServiceImpl(); // 初始化 Lineage 服务
        context.useCatalog(hiveCatalog); // 设置 HiveCatalog 为上下文中的默认 Catalog
    }

    /**
     * 在所有测试结束后执行的清理方法。
     * 关闭 HiveCatalog 连接并删除临时文件夹。
     */
    @AfterClass
    public static void closeCatalog() {
        if (hiveCatalog != null) {
            hiveCatalog.close(); // 关闭 HiveCatalog 连接
        }
        HiveTestUtils.deleteTemporaryFolder(); // 删除临时文件夹
    }

    /**
     * 分析 SQL 的血缘信息，并验证实际结果是否与预期结果一致。
     *
     * @param sql           要分析的 SQL 语句
     * @param expectedArray 预期的血缘结果
     */
    protected void analyzeLineage(String sql, String[][] expectedArray) {
        List<LineageResult> actualList = analyzeLineage(sql); // 分析 SQL 血缘
        List<LineageResult> expectedList = LineageResult.buildResult(catalogName, defaultDatabase, expectedArray); // 构建预期结果
        assertEquals(expectedList, actualList); // 验证结果是否匹配
    }

    /**
     * 分析指定 Catalog 下 SQL 的血缘信息，并验证实际结果是否与预期结果一致。
     *
     * @param catalogName   Catalog 名称
     * @param sql           要分析的 SQL 语句
     * @param expectedArray 预期的血缘结果
     */
    protected void analyzeLineage(String catalogName, String sql, String[][] expectedArray) {
        List<LineageResult> actualList = analyzeLineage(sql);
        List<LineageResult> expectedList = LineageResult.buildResult(catalogName, defaultDatabase, expectedArray);
        assertEquals(expectedList, actualList);
    }

    /**
     * 执行 SQL 血缘分析，并记录结果日志。
     *
     * @param sql 要分析的 SQL 语句
     * @return SQL 血缘结果列表
     */
    private List<LineageResult> analyzeLineage(String sql) {
        List<LineageResult> actualList = context.analyzeLineage(sql);
        LOG.info("Analyze linage result: ");
        actualList.forEach(e -> LOG.info(e.toString())); // 输出每个血缘结果到日志
        return actualList;
    }

    /**
     * 分析 SQL 的功能调用信息，并验证实际结果是否与预期结果一致。
     *
     * @param sql           要分析的 SQL 语句
     * @param expectedArray 预期的功能调用结果
     */
    protected void analyzeFunction(String sql, String[] expectedArray) {
        Set<FunctionResult> actualSet = analyzeFunction(sql);
        Set<FunctionResult> expectedSet = FunctionResult.buildResult(catalogName, defaultDatabase, expectedArray);
        assertEquals(expectedSet, actualSet);
    }

    /**
     * 执行 SQL 功能调用分析，并记录结果日志。
     *
     * @param sql 要分析的 SQL 语句
     * @return SQL 功能调用结果集合
     */
    private Set<FunctionResult> analyzeFunction(String sql) {
        Set<FunctionResult> actualSet = context.analyzeFunction(sql);
        LOG.info("Analyze function result: ");
        actualSet.forEach(e -> LOG.info(e.toString())); // 输出每个功能调用结果到日志
        return actualSet;
    }

    /**
     * 创建 MySQL CDC 表：ods_mysql_users
     */
    protected void createTableOfOdsMysqlUsers() {
        context.execute("DROP TABLE IF EXISTS ods_mysql_users");

        context.execute("CREATE TABLE IF NOT EXISTS ods_mysql_users (" +
                "       id                  BIGINT PRIMARY KEY NOT ENFORCED ," +
                "       name                STRING                          ," +
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
     * 创建 MySQL CDC 表 ods_mysql_users_watermark，用于存储带水印的用户数据。
     * 表定义包含一个基于时间戳字段 `ts` 的水印，以支持事件时间语义。
     */
    protected void createTableOfOdsMysqlUsersWatermark() {
        // 删除已存在的表以避免重复创建错误
        context.execute("DROP TABLE IF EXISTS ods_mysql_users_watermark ");

        // 创建 MySQL CDC 表
        context.execute("CREATE TABLE IF NOT EXISTS ods_mysql_users_watermark (" +
                "       id                  BIGINT PRIMARY KEY NOT ENFORCED , " + // 主键 id
                "       name                STRING                          , " + // 用户名
                "       birthday            TIMESTAMP(3)                    , " + // 用户生日
                "       ts                  TIMESTAMP(3)                    , " + // 时间戳字段
                "       proc_time as proctime()                             , " + // 处理时间
                "       WATERMARK FOR ts AS ts - INTERVAL '5' SECOND         " + // 定义基于 ts 字段的水印，延迟 5 秒
                ") WITH ( " +
                "       'connector' = 'mysql-cdc'            , " + // 使用 MySQL CDC 连接器
                "       'hostname'  = '127.0.0.1'       , " + // 数据库主机地址
                "       'port'      = '3306'                 , " + // 数据库端口
                "       'username'  = 'root'                 , " + // 数据库用户名
                "       'password'  = 'xxx'          , " + // 数据库密码
                "       'server-time-zone' = 'Asia/Shanghai' , " + // 时区设置
                "       'database-name' = 'demo'             , " + // 数据库名称
                "       'table-name'    = 'users' " + // 数据表名称
                ")");
    }

    /**
     * 创建 MySQL 维度表 dim_mysql_company，用于存储用户 ID 和公司名称的映射。
     */
    protected void createTableOfDimMysqlCompany() {
        // 删除已存在的表以避免重复创建错误
        context.execute("DROP TABLE IF EXISTS dim_mysql_company ");

        // 创建 MySQL JDBC 表
        context.execute("CREATE TABLE IF NOT EXISTS dim_mysql_company (" +
                "       user_id                  BIGINT     , " + // 用户 ID
                "       company_name              STRING     " + // 公司名称
                ") WITH ( " +
                "       'connector' = 'jdbc'                 , " + // 使用 JDBC 连接器
                "       'url'       = 'jdbc:mysql://127.0.0.1:3306/demo?useSSL=false&characterEncoding=UTF-8', " + // 数据库连接 URL
                "       'username'  = 'root'                 , " + // 数据库用户名
                "       'password'  = 'xxx'          , " + // 数据库密码
                "       'table-name'= 'company' " + // 数据表名称
                ")");
    }

    /**
     * 创建 Hudi Sink 表 dwd_hudi_users，用于存储用户数据的增量快照，支持分区和复制。
     */
    protected void createTableOfDwdHudiUsers() {
        // 删除已存在的表以避免重复创建错误
        context.execute("DROP TABLE IF EXISTS dwd_hudi_users");

        // 创建 Hudi 表
        context.execute("CREATE TABLE IF NOT EXISTS  dwd_hudi_users ( " +
                "       id                  BIGINT PRIMARY KEY NOT ENFORCED , " + // 主键 id
                "       name                STRING                          , " + // 用户名
                "       company_name        STRING                          , " + // 公司名称
                "       birthday            TIMESTAMP(3)                    , " + // 用户生日
                "       ts                  TIMESTAMP(3)                    , " + // 时间戳字段
                "        `partition`        VARCHAR(20)                      " + // 分区字段
                ") PARTITIONED BY (`partition`) WITH ( " +
                "       'connector' = 'hudi'                                    , " + // 使用 Hudi 连接器
                "       'table.type' = 'COPY_ON_WRITE'                          , " + // Hudi 表类型：写时复制
                "       'path' = '/hudi/users'                                  , " + // Hudi 表存储路径
                "       'read.streaming.enabled' = 'true'                       , " + // 启用流式读取
                "       'read.streaming.check-interval' = '1'                    " + // 流式读取检查间隔
                ")");
    }

    /**
     * 创建 Hudi Sink 表 dws_users_cnt，用于存储用户计数的聚合结果。
     */
    protected void createTableOfDwsHudiUsersCnt() {
        // 删除已存在的表以避免重复创建错误
        context.execute("DROP TABLE IF EXISTS dws_users_cnt");

        // 创建 Hudi 表
        context.execute("CREATE TABLE IF NOT EXISTS  dws_users_cnt ( " +
                "       id                  BIGINT PRIMARY KEY NOT ENFORCED , " + // 主键 id
                "       name_cnt            BIGINT                          , " + // 用户名计数
                "       company_name_cnt    BIGINT                           " + // 公司名称计数
                ") WITH ( " +
                "       'connector' = 'hudi'                                    , " + // 使用 Hudi 连接器
                "       'table.type' = 'MERGE_ON_READ'                           " + // Hudi 表类型：读时合并
                ")");
    }


}
