package com.hw.lineage.flink.paimon;

import com.hw.lineage.flink.basic.AbstractBasicTest;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * @description: PaimonTest 类，用于测试 Paimon Catalog 的创建与表的数据血缘分析功能。
 * 包括表的创建、Join 查询、数据聚合及数据插入操作的验证。
 * Paimon 是一个支持事务的高性能存储引擎，适合于流批一体的数据处理。
 */
public class PaimonTest extends AbstractBasicTest {

    // 定义 Paimon Catalog 的名称
    private static final String catalogName = "paimon";

    // 临时文件夹，用于存储 Paimon Catalog 的仓库路径
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    /**
     * 在每个测试方法执行前调用，创建 Catalog 和表。
     */
    @Before
    public void createCatalogAndTable() throws IOException {
        // 创建并使用Paimon Catalog
        usePaimonCatalog();

        // 创建表 demo_log_01
        createTableOfDemoLog01();

        // 创建表 demo_log_02
        createTableOfDemoLog02();

        // 创建表 demo_log_05
        createTableOfDemoLog05();

        // 创建表 demo_log_agg
        createTableOfDemoLogAgg();
    }

    /**
     * 创建并使用Paimon Catalog
     */
    private void usePaimonCatalog() throws IOException {
        // 使用临时文件夹作为Paimon的仓库路径
        File warehouse = temporaryFolder.newFolder();

        // 创建Paimon Catalog
        context.execute("CREATE CATALOG " + catalogName + " with (          " +
                "       'type' = 'paimon'                                           ," +
                "       'warehouse' = '" + warehouse.toURI() + "'                    " +
                ")");
        // 使用创建的Catalog
        context.execute(String.format("USE CATALOG %s", catalogName));
    }

    /**
     * 测试SQL语句的血缘分析和数据插入操作。
     */
    @Test
    public void testPaimon() {
        // 第一条SQL：将 demo_log_01 和 demo_log_02 的数据 Join 后插入 demo_log_05 表
        String firstSql = "INSERT INTO demo_log_05  " +
                "SELECT                             " +
                "       a.user_id                  ," +
                "       b.item_id                  ," +
                "       b.behavior                 ," +
                "       a.dt                       ," +
                "       a.hh                        " +
                "FROM                               " +
                "       demo_log_01 a               " +
                "LEFT JOIN                          " +
                "       demo_log_02 b               " +
                "ON                                 " +
                "       a.user_id = b.user_id       " +
                "LIMIT 1000                         ";

        // 第一条SQL的预期血缘关系
        String[][] firstExpected = {
                {"demo_log_01", "user_id", "demo_log_05", "user_id"},
                {"demo_log_02", "item_id", "demo_log_05", "item_id"},
                {"demo_log_02", "behavior", "demo_log_05", "behavior"},
                {"demo_log_01", "dt", "demo_log_05", "dt"},
                {"demo_log_01", "hh", "demo_log_05", "hh"}
        };
        // 分析第一条SQL的血缘关系
        analyzeLineage(catalogName, firstSql, firstExpected);

        // 第二条SQL：对 demo_log_05 表的数据进行聚合，并插入 demo_log_agg 表
        String secondSql = "INSERT INTO demo_log_agg " +
                "SELECT                              " +
                "       user_id                     ," +
                "       count(distinct item_id)     ," +
                "       dt                           " +
                "FROM                                " +
                "       demo_log_05                  " +
                "WHERE                               " +
                "       dt='2023-17-02'              " +
                "GROUP BY                            " +
                "       user_id,dt                   ";

        // 第二条SQL的预期血缘关系
        String[][] secondExpected = {
                {"demo_log_05", "user_id", "demo_log_agg", "user_id"},
                {"demo_log_05", "item_id", "demo_log_agg", "cnt", "COUNT(DISTINCT item_id)"},
                {"demo_log_05", "dt", "demo_log_agg", "dt"}
        };
        // 分析第二条SQL的血缘关系
        analyzeLineage(catalogName, secondSql, secondExpected);
    }

    /**
     * 创建表 demo_log_01。
     * 表结构：
     * - user_id：用户ID，主键之一
     * - item_id：商品ID
     * - behavior：用户行为
     * - dt：日期，分区字段之一
     * - hh：小时，分区字段之一
     * 表特性：
     * - 分区字段：dt, hh
     * - 使用4个桶存储数据（bucket = 4）
     */
    private void createTableOfDemoLog01() {
        context.execute("DROP TABLE IF EXISTS demo_log_01 ");

        context.execute("CREATE TABLE IF NOT EXISTS demo_log_01 (   " +
                "       user_id                 BIGINT                       ," +
                "       item_id                 BIGINT                       ," +
                "       behavior                STRING                       ," +
                "       dt                      STRING                       ," +
                "       hh                      STRING                       ," +
                "       PRIMARY KEY (dt, hh, user_id) NOT ENFORCED            " +
                ") PARTITIONED BY (dt, hh) with (                             " +
                "        'bucket' = '4'                                       " +
                ")");
    }

    /**
     * 创建表 demo_log_02，与 demo_log_01 结构相同。
     */
    private void createTableOfDemoLog02() {
        context.execute("DROP TABLE IF EXISTS demo_log_02 ");

        context.execute("CREATE TABLE IF NOT EXISTS demo_log_02 (   " +
                "       user_id                 BIGINT                       ," +
                "       item_id                 BIGINT                       ," +
                "       behavior                STRING                       ," +
                "       dt                      STRING                       ," +
                "       hh                      STRING                       ," +
                "       PRIMARY KEY (dt, hh, user_id) NOT ENFORCED            " +
                ") PARTITIONED BY (dt, hh) with (                             " +
                "        'bucket' = '4'                                       " +
                ")");
    }

    /**
     * 创建表 demo_log_05。
     * 表结构与 demo_log_01 相同，用于存储 Join 后的结果。
     */
    private void createTableOfDemoLog05() {
        context.execute("DROP TABLE IF EXISTS demo_log_05 ");

        context.execute("CREATE TABLE IF NOT EXISTS demo_log_05 (   " +
                "       user_id                 BIGINT                       ," +
                "       item_id                 BIGINT                       ," +
                "       behavior                STRING                       ," +
                "       dt                      STRING                       ," +
                "       hh                      STRING                       ," +
                "       PRIMARY KEY (dt, hh, user_id) NOT ENFORCED            " +
                ") PARTITIONED BY (dt, hh) with (                             " +
                "        'bucket' = '4'                                       " +
                ")");
    }

    /**
     * 创建表 demo_log_agg。
     * 表结构：
     * - user_id：用户ID
     * - cnt：去重后的商品数量
     * - dt：日期，分区字段
     * 表特性：
     * - 分区字段：dt
     * - 使用2个桶存储数据（bucket = 2）
     */
    private void createTableOfDemoLogAgg() {
        context.execute("DROP TABLE IF EXISTS demo_log_agg ");

        context.execute("CREATE TABLE IF NOT EXISTS demo_log_agg (  " +
                "       user_id                 BIGINT                       ," +
                "       cnt                     BIGINT                       ," +
                "       dt                      STRING                       ," +
                "       PRIMARY KEY (dt, user_id) NOT ENFORCED                " +
                ") PARTITIONED BY (dt) with (                                 " +
                "        'bucket' = '2'                                       " +
                ")");
    }
}

