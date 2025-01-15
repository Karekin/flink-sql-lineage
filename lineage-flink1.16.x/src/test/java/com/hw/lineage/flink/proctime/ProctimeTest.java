package com.hw.lineage.flink.proctime;

import com.hw.lineage.flink.basic.AbstractBasicTest;

import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdColumnOrigins;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.junit.Before;
import org.junit.Test;

/**
 * @description: ProctimeTest 类，用于测试带有 PROCTIME() 类型字段的 SQL 解析及血缘关系分析。
 * PROCTIME() 是一种特殊的处理时间字段类型，主要用于流式数据处理中。测试解决了 PROCTIME() 字段在血缘解析中的问题。
 * <p>
 * 测试来源：<a href="https://github.com/HamaWhiteGG/flink-sql-lineage/issues/38">PROCTIME()字段血缘关系解析错误</a>，
 * 通过增强 getColumnOrigins 方法解决该问题。
 */
public class ProctimeTest extends AbstractBasicTest {

    /**
     * 在每个测试用例执行前调用，创建测试所需的表。
     */
    @Before
    public void createTable() {
        // 创建 Kafka 源表 ST
        createTableOfST();

        // 创建带有首字段为 PROCTIME()的打印 Sink 表 TT
        createTableOfWithFirstProcTime();

        // 创建 Datagen 源表 datagen_source
        createTableOfDatagenSource();

        // 创建打印Sink表 print_sink
        createTableOfPrintSink();
    }

    /**
     * 测试场景：将 Kafka 源表 ST 中的数据插入到 Sink 表 TT，包含首字段为 PROCTIME()。
     * <p>
     * 验证点：血缘关系应正确映射，特别是首字段的 PROCTIME()类型字段。
     */
    @Test
    public void testInsertSelectWithFirstProcTimeField() {
        String sql = "INSERT INTO TT(make_time, A, B) " +
                "SELECT " +
                "       make_time               ," +
                "       a                       ," +
                "       b                        " +
                "FROM" +
                "       ST ";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"ST", "make_time", "TT", "make_time"},
                {"ST", "a", "TT", "A"},
                {"ST", "b", "TT", "B"}
        };
        // 分析血缘关系
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 测试场景：将Datagen源表 datagen_source 的数据插入到Sink表 print_sink，包含末字段为PROCTIME()。
     * <p>
     * 验证点：血缘关系应正确映射，尤其是PROCTIME()类型字段的解析。
     */
    @Test
    public void testInsertSelectWithLastProctimeField() {
        String sql = "INSERT INTO print_sink(id, name, make_time) " +
                "SELECT " +
                "       id                  ," +
                "       name                ," +
                "       make_time            " +
                "FROM" +
                "       datagen_source ";

        // 预期的血缘关系
        String[][] expectedArray = {
                {"datagen_source", "id", "print_sink", "id"},
                {"datagen_source", "name", "print_sink", "name"},
                {"datagen_source", "make_time", "print_sink", "make_time"}
        };
        // 分析血缘关系
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 创建Kafka源表 ST。
     * 表结构：
     * - make_time：通过 PROCTIME() 定义的处理时间字段。
     * - a, b：字符串类型字段。
     * 表特性：
     * - 使用Kafka作为数据源。
     * - 配置了JSON格式的消息解析。
     */
    protected void createTableOfST() {
        context.execute("DROP TABLE IF EXISTS ST ");

        context.execute("CREATE TABLE IF NOT EXISTS ST (    " +
                "       make_time       AS PROCTIME()                               ," +
                "       a               STRING                                      ," +
                "       b               STRING                                       " +
                ") WITH (                                                            " +
                "       'connector' = 'kafka',                                       " +
                "       'topic'     = 'user_behavior',                               " +
                "       'properties.bootstrap.servers' = '127.0.0.1:9092',           " +
                "       'properties.group.id'   = 'testGroup',                       " +
                "       'scan.startup.mode'     = 'earliest-offset',                 " +
                "       'format'    = 'json'                                         " +
                ")");
    }

    /**
     * 创建打印Sink表 TT，首字段为 TIMESTAMP(3) 类型。
     * 表结构：
     * - make_time：时间戳字段。
     * - A, B：字符串类型字段。
     * 表特性：
     * - 使用打印Connector，用于调试和展示数据。
     */
    protected void createTableOfWithFirstProcTime() {
        context.execute("DROP TABLE IF EXISTS TT ");

        context.execute("CREATE TABLE IF NOT EXISTS TT (    " +
                "       make_time       TIMESTAMP(3)                                ," +
                "       A               STRING                                      ," +
                "       B               STRING                                       " +
                ") WITH (                                                            " +
                "       'connector' = 'print'                                        " +
                ")");
    }

    /**
     * 创建Datagen源表 datagen_source。
     * 表结构：
     * - id：整型字段。
     * - name：字符串字段。
     * - make_time：通过 PROCTIME() 定义的处理时间字段。
     * 表特性：
     * - 使用DatagenConnector生成数据。
     */
    protected void createTableOfDatagenSource() {
        context.execute("DROP TABLE IF EXISTS datagen_source ");

        context.execute("CREATE TABLE IF NOT EXISTS datagen_source ( " +
                "       id              INT                                  ," +
                "       name            STRING                               ," +
                "       make_time       AS PROCTIME()                         " +
                ") WITH (                                                     " +
                "       'connector' = 'datagen'                               " +
                ")");
    }

    /**
     * 创建打印Sink表 print_sink。
     * 表结构：
     * - id：整型字段。
     * - name：字符串字段。
     * - make_time：时间戳字段。
     * 表特性：
     * - 使用打印Connector，用于调试和展示数据。
     */
    protected void createTableOfPrintSink() {
        context.execute("DROP TABLE IF EXISTS print_sink ");

        context.execute("CREATE TABLE IF NOT EXISTS print_sink (    " +
                "       id              INT                                 ," +
                "       name            STRING                              ," +
                "       make_time       TIMESTAMP(3)                         " +
                ") WITH (                                                    " +
                "       'connector' = 'print'                                " +
                ")");
    }
}
