package com.hw.lineage.flink.tvf;

import com.hw.lineage.flink.basic.AbstractBasicTest;

import org.junit.Before;
import org.junit.Test;

/**
 * @description: TvfTest类，用于测试窗口表值函数（Windowing TVF）在 Flink 中的使用及血缘关系分析。
 * 窗口表值函数（如 TUMBLE、HOP、CUMULATE）用于对时间序列数据进行窗口化处理，支持事件时间和处理时间。
 * 测试覆盖以下场景：
 * - 使用窗口函数生成窗口字段。
 * - 聚合窗口内的数据。
 * - 将处理结果插入到目标表中。
 * 参考文档：<a href="https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/window-tvf">Windowing table-valued functions (Windowing TVFs)</a>
 * </p>
 */
public class TvfTest extends AbstractBasicTest {

    /**
     * 在每个测试用例执行之前，创建测试所需的表。
     */
    @Before
    public void createTable() {
        // 创建 MySQL CDC 表 bid
        createTableOfBid();

        // 创建打印 Sink 表 print_sink
        createTableOfPrintSink();

        // 创建打印 Sink 表 print_sink_agg
        createTableOfPrintSinkAgg();
    }

    /**
     * 测试场景：使用 TUMBLE 窗口函数，窗口化处理数据。
     * <p>
     * SQL 功能：将 MySQL 表 bid 的数据按 10 分钟滚动窗口进行窗口化，并插入到 print_sink 表。
     *
     * RelNode.explain:
     * LogicalProject(bid_time=[$0], price=[$1], item=[$2], window_start=[$3], window_end=[$4], window_time=[$5])
     *   LogicalTableFunctionScan(invocation=[TUMBLE($2, DESCRIPTOR($0), 600000:INTERVAL MINUTE)], rowType=[RecordType(TIMESTAMP(3) *ROWTIME* bid_time, DECIMAL(10, 2) price, VARCHAR(2147483647) item, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)])
     *     LogicalProject(bid_time=[$0], price=[$1], item=[$2])
     *       LogicalWatermarkAssigner(rowtime=[bid_time], watermark=[-($0, 1000:INTERVAL SECOND)])
     *         LogicalTableScan(table=[[hive, default, bid]])
     */
    @Test
    public void testTumble() {
        /*
            这段 SQL 的功能和作用可以总结如下：
            1. 窗口化处理：
               - 使用滚动窗口（每 10 分钟）对 bid 表中的数据进行窗口化。
               - 窗口划分基于 bid_time 列。

            2. 字段扩展：
               - 在原始表的字段基础上，生成额外的窗口字段（如 window_start, window_end, window_time）。

            3. 插入结果：
               - 将窗口化后的所有字段插入到目标表 print_sink，便于调试或进一步分析。
         */
        String sql = "INSERT INTO print_sink                            " +
                "SELECT                                                 " +
                "       *                                               " +
                "FROM TABLE(                                            " +
                "       TUMBLE( TABLE bid                              ," +
                "               DESCRIPTOR(bid_time)                   ," +
                "               INTERVAL '10' MINUTES                   " +
                "       )                                               " +
                ")                                                      ";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"bid", "bid_time", "print_sink", "bid_time"},
                {"bid", "price", "print_sink", "price"},
                {"bid", "item", "print_sink", "item"},
                {"bid", "bid_time", "print_sink", "window_start", "TUMBLE.window_start"},
                {"bid", "bid_time", "print_sink", "window_end", "TUMBLE.window_end"},
                {"bid", "bid_time", "print_sink", "window_time", "TUMBLE.window_time"}
        };

        // 分析血缘关系
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 测试场景：使用 TUMBLE 窗口函数并对窗口内的数据进行聚合。
     * <p>
     * SQL 功能：对 bid 表中的数据按 10 分钟滚动窗口聚合价格，并插入到 print_sink_agg 表。
     */
    @Test
    public void testTumbleAgg() {
        String sql = "INSERT INTO print_sink_agg                        " +
                "SELECT                                                 " +
                "       window_start                                   ," +
                "       window_end                                     ," +
                "       SUM(price)                                      " +
                "FROM TABLE(                                            " +
                "       TUMBLE( TABLE bid                              ," +
                "               DESCRIPTOR(bid_time)                   ," +
                "               INTERVAL '10' MINUTES                   " +
                "       )                                               " +
                ")                                                      " +
                "GROUP BY                                               " +
                "       window_start, window_end                        ";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"bid", "bid_time", "print_sink_agg", "window_start", "TUMBLE.window_start"},
                {"bid", "bid_time", "print_sink_agg", "window_end", "TUMBLE.window_end"},
                {"bid", "price", "print_sink_agg", "price", "SUM(price)"}
        };

        // 分析血缘关系
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 测试场景：使用 HOP 窗口函数。
     * <p>
     * SQL 功能：对 bid 表按 5 分钟滑动窗口处理，并插入到 print_sink 表。
     */
    @Test
    public void testHop() {
        String sql = "INSERT INTO print_sink                            " +
                "SELECT                                                 " +
                "       *                                               " +
                "FROM TABLE(                                            " +
                "       HOP( TABLE bid                                 ," +
                "               DESCRIPTOR(bid_time)                   ," +
                "               INTERVAL '5' MINUTES                   ," +
                "               INTERVAL '10' MINUTES                   " +
                "       )                                               " +
                ")                                                      ";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"bid", "bid_time", "print_sink", "bid_time"},
                {"bid", "price", "print_sink", "price"},
                {"bid", "item", "print_sink", "item"},
                {"bid", "bid_time", "print_sink", "window_start", "HOP.window_start"},
                {"bid", "bid_time", "print_sink", "window_end", "HOP.window_end"},
                {"bid", "bid_time", "print_sink", "window_time", "HOP.window_time"}
        };

        // 分析血缘关系
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 测试场景：使用 HOP 窗口函数并对窗口内的数据进行聚合。
     * <p>
     * SQL 功能：对 bid 表按 5 分钟滑动窗口聚合价格，并插入到 print_sink_agg 表。
     */
    @Test
    public void testHopAgg() {
        String sql = "INSERT INTO print_sink_agg                        " +
                "SELECT                                                 " +
                "       window_start                                   ," +
                "       window_end                                     ," +
                "       SUM(price)                                      " +
                "FROM TABLE(                                            " +
                "       HOP( TABLE bid                                 ," +
                "               DESCRIPTOR(bid_time)                   ," +
                "               INTERVAL '5' MINUTES                   ," +
                "               INTERVAL '10' MINUTES                   " +
                "       )                                               " +
                ")                                                      " +
                "GROUP BY                                               " +
                "       window_start, window_end                        ";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"bid", "bid_time", "print_sink_agg", "window_start", "HOP.window_start"},
                {"bid", "bid_time", "print_sink_agg", "window_end", "HOP.window_end"},
                {"bid", "price", "print_sink_agg", "price", "SUM(price)"}
        };

        // 分析血缘关系
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 测试场景：使用 CUMULATE 窗口函数。
     * <p>
     * SQL 功能：对 bid 表按累积窗口（2分钟步长，10分钟窗口）处理数据，并插入到 print_sink 表。
     */
    @Test
    public void testCumulate() {
        String sql = "INSERT INTO print_sink                            " +
                "SELECT                                                 " +
                "       *                                               " +
                "FROM TABLE(                                            " +
                "       CUMULATE( TABLE bid                            ," +
                "               DESCRIPTOR(bid_time)                   ," +
                "               INTERVAL '2' MINUTES                   ," +
                "               INTERVAL '10' MINUTES                   " +
                "       )                                               " +
                ")                                                      ";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"bid", "bid_time", "print_sink", "bid_time"},
                {"bid", "price", "print_sink", "price"},
                {"bid", "item", "print_sink", "item"},
                {"bid", "bid_time", "print_sink", "window_start", "CUMULATE.window_start"},
                {"bid", "bid_time", "print_sink", "window_end", "CUMULATE.window_end"},
                {"bid", "bid_time", "print_sink", "window_time", "CUMULATE.window_time"}
        };

        // 分析血缘关系
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 测试场景：使用 CUMULATE 窗口函数并对窗口内的数据进行聚合。
     * <p>
     * SQL 功能：对 bid 表按累积窗口聚合价格，并插入到 print_sink_agg 表。
     */
    @Test
    public void testCumulateAgg() {
        String sql = "INSERT INTO print_sink_agg                        " +
                "SELECT                                                 " +
                "       window_start                                   ," +
                "       window_end                                     ," +
                "       SUM(price)                                      " +
                "FROM TABLE(                                            " +
                "       CUMULATE( TABLE bid                            ," +
                "               DESCRIPTOR(bid_time)                   ," +
                "               INTERVAL '2' MINUTES                   ," +
                "               INTERVAL '10' MINUTES                   " +
                "       )                                               " +
                ")                                                      " +
                "GROUP BY                                               " +
                "       window_start, window_end                        ";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"bid", "bid_time", "print_sink_agg", "window_start", "CUMULATE.window_start"},
                {"bid", "bid_time", "print_sink_agg", "window_end", "CUMULATE.window_end"},
                {"bid", "price", "print_sink_agg", "price", "SUM(price)"}
        };

        // 分析血缘关系
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 创建 MySQL CDC 表 bid。
     * 表结构：
     * - bid_time：时间戳字段，用于窗口处理。
     * - price：价格字段。
     * - item：商品名称。
     * 配置：
     * - 使用 MySQL CDC 作为数据源。
     * - 配置事件时间 Watermark。
     */
    protected void createTableOfBid() {
        context.execute("DROP TABLE IF EXISTS bid ");
        context.execute("CREATE TABLE IF NOT EXISTS bid (                   " +
                "       bid_time            TIMESTAMP(3)                            ," +
                "       price               DECIMAL(10, 2)                          ," +
                "       item                STRING                                  ," +
                "       WATERMARK FOR bid_time AS bid_time - INTERVAL '1' SECOND     " +
                ") WITH (                                     " +
                "       'connector' = 'mysql-cdc'            ," +
                "       'hostname'  = '127.0.0.1'            ," +
                "       'port'      = '3306'                 ," +
                "       'username'  = 'root'                 ," +
                "       'password'  = '123456'                  ," +
                "       'server-time-zone' = 'Asia/Shanghai' ," +
                "       'database-name' = 'demo'             ," +
                "       'table-name'    = 'users'            ," +
                "       'scan.incremental.snapshot.enabled' = 'false'   " +
                ")");
    }

    /**
     * 创建打印 Sink 表 print_sink，用于窗口化数据的存储。
     */
    protected void createTableOfPrintSink() {
        context.execute("DROP TABLE IF EXISTS print_sink ");

        context.execute("CREATE TABLE IF NOT EXISTS print_sink (    " +
                "       bid_time            TIMESTAMP(3)                    ," +
                "       price               DECIMAL(10, 2)                  ," +
                "       item                STRING                          ," +
                "       window_start        TIMESTAMP(3)                    ," +
                "       window_end          TIMESTAMP(3)                    ," +
                "       window_time         TIMESTAMP(3)                     " +
                ") WITH (                                                    " +
                "       'connector' = 'print'                                " +
                ")");
    }

    /**
     * 创建打印 Sink 表 print_sink_agg，用于聚合结果的存储。
     */
    protected void createTableOfPrintSinkAgg() {
        context.execute("DROP TABLE IF EXISTS print_sink_agg ");

        context.execute("CREATE TABLE IF NOT EXISTS print_sink_agg ( " +
                "       window_start        TIMESTAMP(3)                     ," +
                "       window_end          TIMESTAMP(3)                     ," +
                "       price               DECIMAL(10, 2)                    " +
                ") WITH (                                                     " +
                "       'connector' = 'print'                                 " +
                ")");
    }
}
