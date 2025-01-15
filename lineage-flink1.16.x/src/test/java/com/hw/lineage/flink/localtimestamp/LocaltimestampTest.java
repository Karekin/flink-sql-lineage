package com.hw.lineage.flink.localtimestamp;

import com.hw.lineage.flink.basic.AbstractBasicTest;

import org.junit.Before;
import org.junit.Test;

/**
 * @description: LocaltimestampTest 类，用于测试在 Flink SQL 中使用 LOCALTIMESTAMP 生成的时间字段及其数据血缘关系。
 * 测试场景：
 * - 从 datagen 表（包含 LOCALTIMESTAMP 生成的字段）中读取数据。
 * - 将数据写入到目标打印表（print_sink）。
 * LOCALTIMESTAMP 是 Flink 提供的一种内置函数，用于生成当前系统时间的本地时间戳。
 * </p>
 */
public class LocaltimestampTest extends AbstractBasicTest {

    /**
     * 在每个测试用例执行之前，创建测试所需的表。
     */
    @Before
    public void createTable() {
        // 创建数据生成源表 datagen_source
        createTableOfDatagenSource();

        // 创建打印 Sink 表 print_sink
        createTableOfPrintSink();
    }

    /**
     * 测试场景：从包含 LOCALTIMESTAMP 字段的数据源表读取数据并写入到打印 Sink 表。
     * <p>
     * SQL 功能：从 datagen_source 表中选择所有字段，并将其插入到 print_sink 表中。
     */
    @Test
    public void testInsertSelectLocaltimestamp() {
        String sql = "INSERT INTO print_sink " +
                "SELECT " +
                "       id          ," +
                "       name        ," +
                "       birthday    ," +
                "       ts           " +
                "FROM" +
                "       datagen_source ";

        // 预期的血缘关系：定义源字段与目标字段的映射关系
        String[][] expectedArray = {
                {"datagen_source", "id", "print_sink", "id"},
                {"datagen_source", "name", "print_sink", "name"},
                {"datagen_source", "birthday", "print_sink", "birthday"},
                {"datagen_source", "ts", "print_sink", "ts"}
        };

        // 分析血缘关系
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 创建数据生成源表 datagen_source。
     * <p>
     * 表结构：
     * - id：整数类型，表示主键或唯一标识。
     * - name：字符串类型，表示名称。
     * - birthday：基于 LOCALTIMESTAMP 自动生成的本地时间戳，表示生日。
     * - ts：基于 LOCALTIMESTAMP 自动生成的本地时间戳，表示时间戳。
     * <p>
     * 表配置：
     * - 使用 'datagen' 连接器生成随机数据。
     * - ts 列设置水印，用于支持基于事件时间的流式处理。
     */
    protected void createTableOfDatagenSource() {
        context.execute("DROP TABLE IF EXISTS datagen_source ");

        context.execute("CREATE TABLE IF NOT EXISTS datagen_source (" +
                "       id              INT                        ," +
                "       name            STRING                     ," +
                "       birthday        AS LOCALTIMESTAMP          ," +
                "       ts              AS LOCALTIMESTAMP          ," +
                "       WATERMARK FOR ts AS ts                      " +
                ") WITH ( " +
                "       'connector' = 'datagen'                     " +
                ")");
    }

    /**
     * 创建打印 Sink 表 print_sink。
     * <p>
     * 表结构：
     * - id：整数类型，表示主键或唯一标识。
     * - name：字符串类型，表示名称。
     * - birthday：TIMESTAMP(3) 类型，表示生日。
     * - ts：TIMESTAMP(3) 类型，表示时间戳。
     * <p>
     * 表配置：
     * - 使用 'print' 连接器，将数据输出到控制台，用于调试或查看结果。
     */
    protected void createTableOfPrintSink() {
        context.execute("DROP TABLE IF EXISTS print_sink ");

        context.execute("CREATE TABLE IF NOT EXISTS print_sink (" +
                "       id              INT                         ," +
                "       name            STRING                      ," +
                "       birthday        TIMESTAMP(3)                ," +
                "       ts              TIMESTAMP(3)                 " +
                ") WITH ( " +
                "       'connector' = 'print'                        " +
                ")");
    }
}

