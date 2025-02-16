package com.hw.lineage.flink.cep;

import com.hw.lineage.flink.basic.AbstractBasicTest;

import org.junit.Before;
import org.junit.Test;

/**
 * 测试用例来自以下两个链接：
 * 1. <a href="https://www.jianshu.com/p/c0b76abe4224">CEP used by Flink (SQL method)</a>
 * 2. <a href="https://github.com/HamaWhiteGG/flink-sql-lineage/issues/62">[Bug]Flink cep sql column lineage parse error</a>
 *
 * 该测试类主要用于演示 Flink 中的 CEP（Complex Event Processing）功能在 SQL 方式下的使用方式，
 * 并测试字段血缘（lineage）解析过程是否符合预期。
 *
 * @description: CepTest
 */
public class CepTest extends AbstractBasicTest {

    /**
     * 在每个测试方法执行之前，会先创建相应的表，包括数据源表和结果表：
     * 1. temperature_source : 通过 CSV 文件模拟温度传感器数据的源表。
     * 2. print_sink : 将结果打印到控制台的目的表。
     * 3. source_table : DataGen 类型的源表，用于生成测试数据。
     * 4. sink_table : blackhole 类型的目标表，用于验证 CEP 结果但不做实际输出。
     */
    @Before
    public void createTable() {
        // 创建 temperature_source 表，模拟温度传感器的数据源
        createTableOfTemperatureSource();

        // 创建 print_sink 表，用于测试 CEP 结果输出到控制台
        createTableOfPrintSink();

        // 创建 source_table 表，DataGen 方式生成模拟数据
        createTableOfSourceTable();

        // 创建 sink_table 表，blackhole 方式，用于验证 CEP 结果
        createTableOfSinkTable();
    }

    /**
     * 测试将 CEP 查询结果插入到 print_sink 表中：
     * 1. temperature_source 表作为 MATCH_RECOGNIZE 的数据源；
     * 2. MATCH_RECOGNIZE 通过定义模式 (A B+ C) 来匹配温度高于、低于特定阈值的序列；
     * 3. 提取 ts、temperature 等字段，通过 LAST() 和 AVG() 等函数计算结果；
     * 4. 将结果插入到 print_sink 表并验证字段血缘是否正确解析。
     *
     * RelNode explain：
     * LogicalProject(rack_id=[$0], start_ts=[$1], end_ts=[$2], start_temp=[$3], end_temp=[$4], avg_temp=[$5])
     *   LogicalMatch(partition=[[0]], order=[[1 ASC-nulls-first]], outputFields=[[rack_id, start_ts, end_ts, start_temp, end_temp, avg_temp]], allRows=[false], after=[FLAG(SKIP TO NEXT ROW)], pattern=[((_UTF-16LE'A', PATTERN_QUANTIFIER(_UTF-16LE'B', 1, -1, false)), _UTF-16LE'C')], isStrictStarts=[false], isStrictEnds=[false], interval=[90000:INTERVAL SECOND], subsets=[[]], patternDefinitions=[[<(PREV(A.$2, 0), 50), >=(PREV(B.$2, 0), 50), <(PREV(C.$2, 0), 50)]], inputFields=[[rack_id, ts, temperature]])
     *     LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($1, 1000:INTERVAL SECOND)])
     *       LogicalTableScan(table=[[hive, default, temperature_source]])
     */
    @Test
    public void testInsertSelectCep() {
        // 匹配模式：A(temperature<50), B(temperature>=50)，C(temperature<50)
        // 当匹配到上述模式时，将测量结果输出到 print_sink 表
        String sql = "INSERT INTO print_sink " +
                "SELECT " +
                "    * " +
                "FROM temperature_source " +
                "MATCH_RECOGNIZE ( " +
                "    PARTITION BY rack_id " +
                "    ORDER BY ts " +
                "    MEASURES " +
                "        A.ts AS start_ts, " +
                "        LAST(B.ts) AS end_ts, " +
                "        A.temperature AS start_temp, " +
                "        LAST(B.temperature) AS end_temp, " +
                "        AVG(B.temperature) AS avg_temp " +
                "    ONE ROW PER MATCH " +
                "    AFTER MATCH SKIP TO NEXT ROW " +
                "    PATTERN (A B+ C) WITHIN INTERVAL '90' SECOND " +
                "    DEFINE " +
                "        A AS A.temperature < 50, " +
                "        B AS B.temperature >= 50, " +
                "        C AS C.temperature < 50 " +
                ")";

        // 预期的字段血缘，二维数组中每个条目依次表示：
        // { 源表, 源字段, 目标表, 目标字段, (可选)表达式信息 }
        String[][] expectedArray = {
                {"temperature_source", "rack_id", "print_sink", "rack_id"},
                {"temperature_source", "ts", "print_sink", "start_ts", "A.ts"},
                {"temperature_source", "ts", "print_sink", "end_ts", "LAST(B.ts, 0)"},
                {"temperature_source", "temperature", "print_sink", "start_temp", "A.temperature"},
                {"temperature_source", "temperature", "print_sink", "end_temp", "LAST(B.temperature, 0)"},
                {"temperature_source", "temperature", "print_sink", "avg_temp",
                        "CAST(/(SUM(B.temperature), COUNT(B.temperature))):INTEGER"}
        };

        // 调用 analyzeLineage 方法来验证字段血缘结果
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 测试将 CEP 查询结果先创建 VIEW（temperature_view），
     * 再从 VIEW 中 SELECT 出来插入到 print_sink 表：
     * 1. 先删除已有的 temperature_view（如果存在）；
     * 2. 通过 MATCH_RECOGNIZE 创建 temperature_view；
     * 3. 将该视图中的字段插入到 print_sink；
     * 4. 最终验证血缘解析是否正确。
     */
    @Test
    public void testInsertSelectViewCep() {
        // 如果已经存在 temperature_view，则先删除它
        context.execute("DROP VIEW IF EXISTS temperature_view ");

        // 创建一个视图 temperature_view，通过 MATCH_RECOGNIZE 来匹配事件序列

        /*
            这段 SQL 的主要作用是：创建一个名为 temperature_view 的视图，它并不直接保存数据，
            而是基于表 temperature_source 上的 CEP 模式匹配 结果进行投影（SELECT）。
            你可以理解为当你查询这个视图时，实际上是在执行该 MATCH_RECOGNIZE 操作，并返回匹配的结果数据。

            一、关键点拆解

            1. MATCH_RECOGNIZE 子句

            - PARTITION BY rack_id
              将数据按照 rack_id 进行分区，也就意味着不同的 rack_id 之间是相互独立的匹配序列。Flink 会为每个分区单独进行模式匹配。

            - ORDER BY ts
              指定匹配所使用的事件时间顺序，这里按 ts 字段（时间戳）进行排序。也就是说，在一个分区内数据是按照时间先后顺序被处理的。

            2. PATTERN (A B+ C) WITHIN INTERVAL '90' second

            - PATTERN (A B+ C)
              - 该模式表示先出现一个满足 A 条件的事件，然后紧随一个或多个满足 B 条件的事件，最后出现一个满足 C 条件的事件。
              - B+ 表示一个或多个 B 事件连续出现。

            - WITHIN INTERVAL '90' second
              - 指定事件匹配的时间窗口：如果从捕捉到 A 事件开始，到匹配到 C 事件结束的时间跨度超过 90 秒，则视为不匹配。
                只有在 90 秒内完整出现了 (A B+ C) 这个序列，才算一次完整匹配。

            3. DEFINE

            - 这里定义了模式 A, B, C 对应的匹配条件：
              - A AS A.temperature < 50
                表示事件 A 的温度要低于 50。
              - B AS B.temperature >= 50
                表示事件 B 的温度大于或等于 50。
              - C AS C.temperature < 50
                表示事件 C 的温度再次降到 50 以下。

            因此，当我们在同一个 rack_id 分区内，按照时间顺序找到一个事件温度 < 50，
            接着一个或多个事件温度 >= 50，最后又有一个事件温度 < 50，且在 90 秒内完成，则会触发一次匹配。

            4. MEASURES

            - MEASURES 指定：当成功匹配到模式 (A B+ C) 时，如何从匹配的事件序列里提取字段进行输出。
            - 具体字段：
              - A.ts AS start_ts：把模式 A 事件的时间戳当作 start_ts。
              - LAST(B.ts) AS end_ts：把最后一个 B 事件的时间戳当作 end_ts。因为 B+ 是多个 B 事件，因此需要用 LAST() 来获取最后一个匹配的 B 事件的时间戳。
              - A.temperature AS start_temp：获取模式 A 事件的温度，命名为 start_temp。
              - LAST(B.temperature) AS end_temp：获取最后一个 B 事件的温度，命名为 end_temp。
              - AVG(B.temperature) AS avg_temp：对所有 B 事件的温度做平均值，命名为 avg_temp。

            > 注意：这里没有直接提到 C 的字段，是因为本例只提取了 A 和 B 中的字段信息。当然，如果需要，也可以在 MEASURES 中对 C 的字段进行聚合或提取。

            5. ONE ROW PER MATCH 与 AFTER MATCH SKIP TO NEXT ROW

            - ONE ROW PER MATCH：每一次完整匹配到 (A B+ C) 模式时，都输出一行匹配结果。
            - AFTER MATCH SKIP TO NEXT ROW：当一次匹配完成后，下一次匹配从下一个事件开始。
              如果你希望不跳过任何事件或者跳过匹配的最后一个事件等，还可以使用其他跳过策略（如 SKIP PAST LAST ROW、SKIP TO FIRST B 等）。

            二、小结

            1. 分区 (PARTITION BY)：保证不同 rack_id 各自进行匹配，互不影响。
            2. 排序 (ORDER BY)：决定了匹配事件的先后顺序（基于 ts）。
            3. 定义模式 (PATTERN)：指定了事件匹配的顺序和结构 (A B+ C)，以及时间范围 WITHIN 90 秒。
            4. 条件 (DEFINE)：为 A、B、C 分别设定了温度大小的过滤条件。
            5. 提取结果 (MEASURES)：提取匹配到的事件时间和温度，并进行聚合计算 (LAST, AVG)。
            6. 输出策略 (ONE ROW PER MATCH ...)：每次完整匹配后输出一行结果，并从下一行事件继续。

            通过这段 SQL，我们可以捕捉温度在 50 以下 -> 连续若干次 50 以上 -> 再次低于 50 这一“升温-保持-降温”的模式，
            从而得到温度变化区间的起止时间（start_ts/end_ts）及温度信息（start_temp/end_temp/avg_temp）。
            在业务中，这常用来定位某种连续状态变化，比如报警或温度异常区间等。
         */
        context.execute("CREATE VIEW IF NOT EXISTS temperature_view AS " +
                "SELECT                                                     " +
                "   *                                                       " +
                "FROM                                                       " +
                "   temperature_source MATCH_RECOGNIZE (                    " +
                "       PARTITION BY rack_id                                " +
                "       ORDER BY ts                                         " +
                "       MEASURES                                            " +
                "           A.ts as start_ts,                               " +
                "           LAST(B.ts) as end_ts,                           " +
                "           A.temperature as start_temp,                    " +
                "           LAST(B.temperature) as end_temp,                " +
                "           AVG(B.temperature) as avg_temp                  " +
                "           ONE ROW PER MATCH                               " +
                "           AFTER MATCH SKIP TO NEXT ROW                    " +
                "           PATTERN (A B+ C) WITHIN INTERVAL '90' second    " +
                "           DEFINE                                          " +
                "               A as A.temperature < 50,                    " +
                "               B as B.temperature >=50,                    " +
                "               C as C.temperature < 50                     " +
                "   )");

        // 从 temperature_view 中选取特定字段插入到 print_sink
        String sql = "INSERT INTO print_sink (rack_id, start_ts, end_ts, start_temp, end_temp, avg_temp) " +
                "SELECT " +
                "   rack_id     ," +
                "   start_ts    ," +
                "   end_ts      ," +
                "   start_temp  ," +
                "   end_temp    ," +
                "   avg_temp     " +
                "FROM" +
                "   temperature_view";

        // 视图中的字段对应到 print_sink 表字段的血缘映射
        String[][] expectedArray = {
                {"temperature_source", "rack_id", "print_sink", "rack_id"},
                {"temperature_source", "ts", "print_sink", "start_ts", "CAST(A.ts):TIMESTAMP(3)"},
                {"temperature_source", "ts", "print_sink", "end_ts", "CAST(LAST(B.ts, 0)):TIMESTAMP(3)"},
                {"temperature_source", "temperature", "print_sink", "start_temp", "A.temperature"},
                {"temperature_source", "temperature", "print_sink", "end_temp", "LAST(B.temperature, 0)"},
                {"temperature_source", "temperature", "print_sink", "avg_temp",
                        "CAST(/(SUM(B.temperature), COUNT(B.temperature))):INTEGER"}
        };

        // 调用 analyzeLineage 方法来验证字段血缘结果
        analyzeLineage(sql, expectedArray);
    }

    /**
     * 创建 temperature_source 表，用于模拟温度传感器的数据。
     * 包含以下字段：
     * 1. rack_id : 机架编号
     * 2. ts : 时间戳（TIMESTAMP(3)），并设置 watermark
     * 3. temperature : 温度（整型）
     * 4. WATERMARK FOR ts : 1 秒的延迟来生成 watermark
     * 使用 filesystem connector 和 csv 格式读取 data/temperature_record.csv 文件中的数据。
     */
    protected void createTableOfTemperatureSource() {
        // 如果 temperature_source 表已存在，则先删除
        context.execute("DROP TABLE IF EXISTS temperature_source ");

        // 创建 temperature_source 表，指定各字段类型并设置 watermark 策略
        context.execute("CREATE TABLE IF NOT EXISTS temperature_source (" +
                "       rack_id                 INT                         ," +
                "       ts                      TIMESTAMP(3)                ," +
                "       temperature             INT                         ," +
                "       WATERMARK FOR ts AS ts - INTERVAL '1' SECOND         " +
                ") WITH (                                                    " +
                "       'connector' = 'filesystem'                          ," +
                "       'path' = 'data/temperature_record.csv'              ," +
                "       'format' = 'csv'                                     " +
                ")                                                           ");
    }

    /**
     * 创建 print_sink 表，用于将查询结果打印到控制台。
     * 包含以下字段：
     * 1. rack_id
     * 2. start_ts
     * 3. end_ts
     * 4. start_temp
     * 5. end_temp
     * 6. avg_temp
     * 使用 print connector 来直接打印结果。
     */
    protected void createTableOfPrintSink() {
        // 如果 print_sink 表已存在，则先删除
        context.execute("DROP TABLE IF EXISTS print_sink ");

        // 创建 print_sink 表，并指定字段
        context.execute("CREATE TABLE IF NOT EXISTS print_sink (    " +
                "       rack_id             INT                             ," +
                "       start_ts            TIMESTAMP(3)                    ," +
                "       end_ts              TIMESTAMP(3)                    ," +
                "       start_temp          INT                             ," +
                "       end_temp            INT                             ," +
                "       avg_temp            INT                              " +
                ") WITH (                                                    " +
                "       'connector' = 'print'                                " +
                ")                                                           ");
    }

    /**
     * 创建 source_table 表，使用 DataGen 连接器来随机生成数据。
     * 包含以下字段：
     * 1. agent_id
     * 2. room_id
     * 3. create_time
     * 4. type
     * 5. application_id
     * 6. connect_time
     * 7. row_time : proctime，用于处理时间
     */
    protected void createTableOfSourceTable() {
        // 如果 source_table 表已存在，则先删除
        context.execute("DROP TABLE IF EXISTS source_table ");

        // 创建 source_table 表，指定各字段类型及 DataGen 连接器
        context.execute("CREATE TABLE IF NOT EXISTS source_table ( " +
                "       agent_id            STRING                          ," +
                "       room_id             STRING                          ," +
                "       create_time         BIGINT                          ," +
                "       type                STRING                          ," +
                "       application_id      STRING                          ," +
                "       connect_time        BIGINT                          ," +
                "       row_time AS PROCTIME()                               " +
                ") WITH (                                                    " +
                "       'connector' = 'datagen'                              " +
                ")                                                           ");
    }

    /**
     * 创建 sink_table 表，使用 blackhole 连接器，数据写入后直接丢弃（不进行实际输出）。
     * 包含以下字段：
     * 1. agent_id
     * 2. room_id
     * 3. application_id
     * 4. type
     * 5. begin_time
     * 6. end_time
     */
    protected void createTableOfSinkTable() {
        // 如果 sink_table 表已存在，则先删除
        context.execute("DROP TABLE IF EXISTS sink_table ");

        // 创建 sink_table 表，使用 blackhole 连接器
        context.execute("CREATE TABLE IF NOT EXISTS sink_table (    " +
                "       agent_id            STRING                          ," +
                "       room_id             STRING                          ," +
                "       application_id      STRING                          ," +
                "       type                STRING                          ," +
                "       begin_time          BIGINT                          ," +
                "       end_time            BIGINT                           " +
                ") WITH (                                                    " +
                "       'connector' = 'blackhole'                            " +
                ")                                                           ");
    }

    /**
     * 测试多分区键（PARTITION BY agent_id, room_id, application_id）的 CEP 应用：
     * 1. 在 source_table 上使用 MATCH_RECOGNIZE；
     * 2. 匹配模式 BF(type='assign') + AF(type='pick_up')；
     * 3. 提取相应字段并插入到 sink_table 中进行验证；
     * 4. 检查字段血缘是否与预期相符。
     */
    @Test
    public void testInsertSelectCepWithMultiplePartitionKeys() {
        // CEP 语句，定义了多分区键和模式 BF+AF
        // BF 表示 type = 'assign'，AF 表示 type = 'pick_up'

        /*
            一、整体 SQL 回顾

            该语句的意图是：从 source_table 中识别符合特定事件模式 (BF+ AF) 的数据序列，
            并将匹配结果插入 sink_table。其中 BF 和 AF 是自定义的事件变量名称，用于区分不同的事件类型（type）。

            1. PARTITION BY 与 ORDER BY

            - PARTITION BY agent_id, room_id, application_id
              将数据以 (agent_id, room_id, application_id) 的组合进行分区。换句话说，对于每一种 (agent_id, room_id, application_id) 组合，都会单独进行一次模式匹配，不同分区互不影响。

            - ORDER BY row_time
              在每个分区内，按照 row_time 字段顺序来处理和匹配事件。这通常是一个处理时间或事件时间的字段，表示事件到达或发生的时间顺序。

            2. PATTERN (BF+ AF) WITHIN INTERVAL '1' HOUR

            - PATTERN (BF+ AF)
              - BF+ 表示 “一个或多个 BF 事件” 连续出现；
              - AF 表示 “一个 AF 事件”；
              - 因此模式 (BF+ AF) 要求先出现一个或多个 BF 事件，然后紧跟一个 AF 事件，才算匹配成功。

            - WITHIN INTERVAL '1' HOUR
              - 表示对这组匹配的事件序列有一个时间窗口限制：从第一个 BF 出现开始，到 AF 出现结束的总时长不能超过 1 小时。如果超时，则无法构成一次完整的匹配。

            3. DEFINE

            - 这里给 BF 和 AF 定义了一个布尔条件，用来筛选满足各自条件的事件。
              - BF：BF.type = 'assign'
                即对于类型为 'assign' 的事件，都可视为 BF 事件。
              - AF：AF.type = 'pick_up'
                即对于类型为 'pick_up' 的事件，都可视为 AF 事件。

            - 结合前面的 BF+，可以理解为：在同一个分区内，只要连续出现一个或多个 type='assign' 的事件（BF），然后紧跟一个 type='pick_up' 的事件（AF），就会触发一次匹配。

            4. MEASURES

            - MEASURES 子句用于从匹配成功的事件序列中提取需要的信息，映射为输出字段：
              - AF.type AS type
                使用匹配到的 AF 事件的 type 字段，重命名为 type。
              - LAST(BF.create_time) AS begin_time
                因为 BF+ 可能有多个事件，LAST(BF.create_time) 代表最后一个 BF 事件的 create_time，将其作为匹配序列的 begin_time。
              - LAST(AF.create_time) AS end_time
                由于 AF 只有一个事件，但习惯上使用 LAST(...) 可以保持写法一致，将该事件的 create_time 命名为 end_time。

            - 因为我们在 SELECT 中写的是 SELECT agent_id, room_id, application_id, type, begin_time, end_time ...，对应的值就是通过 MEASURES 提取或保留的字段。

            5. ONE ROW PER MATCH 和 AFTER MATCH SKIP PAST LAST ROW

            - ONE ROW PER MATCH
              - 当匹配到模式 (BF+ AF) 一次，就输出一行结果（也就是一条匹配记录）。

            - AFTER MATCH SKIP PAST LAST ROW
              - 指定在一次匹配完成后，从最后一个匹配到的事件之后继续寻找下一次匹配。
              - 这样可以避免相同事件被重复匹配，或无限循环匹配。

            6. 整体流程

            1. 分区：以 (agent_id, room_id, application_id) 组合拆分数据流，分别处理。
            2. 顺序：在每个分区内，事件先按 row_time 顺序排序。
            3. 模式匹配：找到一段时间（不超过 1 小时）内，先出现 一个或多个 type='assign' 的事件（BF），然后紧跟一个 type='pick_up' 的事件（AF）。
            4. 条件：通过 DEFINE 中的表达式来判断事件的类型。
            5. 输出：对于每一次完整匹配，提取：
               - 最后一个 BF 的 create_time 作为 begin_time；
               - AF 的 create_time 作为 end_time；
               - AF 的 type 作为 type；
               - 以及分区字段 agent_id, room_id, application_id；
               - 最终输出到 sink_table。

            7. 总结

            - 这段 CEP SQL 适用于捕捉：在同一个 (agent_id, room_id, application_id) 分区内，是否先发生了一段连续的 “assign” 事件（BF+），然后紧接着出现了 “pick_up” 事件（AF），并且整个过程在 1 小时内完成。如果符合，则输出一条匹配结果到 sink_table。
            - 通过 MEASURES，可以自定义想要提取哪些字段以及如何聚合/计算（如 LAST, FIRST, AVG, COUNT 等）。
            - ONE ROW PER MATCH 让每次匹配输出一行；AFTER MATCH SKIP PAST LAST ROW 避免重复或重叠匹配。

            这就是 SQL 中 CEP 语法的核心意义：在流或表的数据中基于事件类型和时间规则找到某一特定序列，然后提取它并输出。在实际场景中，往往用来监控或追踪事件的先后顺序，比如订单流程、状态机转换等。
         */
        String sql = "INSERT INTO sink_table (agent_id, room_id, application_id, type, begin_time, end_time)    " +
                "SELECT                                                                                         " +
                "   agent_id                                                                                   ," +
                "   room_id                                                                                    ," +
                "   application_id                                                                             ," +
                "   type                                                                                       ," +
                "   begin_time                                                                                 ," +
                "   end_time                                                                                    " +
                "FROM                                                                                           " +
                "   source_table MATCH_RECOGNIZE (                                                              " +
                "       PARTITION BY agent_id, room_id, application_id                                          " +
                "       ORDER BY row_time                                                                       " +
                "       MEASURES                                                                                " +
                "           AF.type as type                                                                    ," +
                "           LAST(BF.create_time) as begin_time                                                 ," +
                "           LAST(AF.create_time) as end_time                                                    " +
                "           ONE ROW PER MATCH                                                                   " +
                "           AFTER MATCH SKIP PAST LAST ROW                                                      " +
                "           PATTERN (BF+ AF) WITHIN INTERVAL '1' HOUR                                           " +
                "           DEFINE                                                                              " +
                "               BF as BF.type = 'assign'                                                       ," +
                "               AF as AF.type = 'pick_up'                                                       " +
                "   ) as T                                                                                      ";

        // 预期的字段血缘映射关系
        String[][] expectedArray = {
                {"source_table", "agent_id", "sink_table", "agent_id"},
                {"source_table", "room_id", "sink_table", "room_id"},
                {"source_table", "application_id", "sink_table", "application_id"},
                {"source_table", "type", "sink_table", "type", "FINAL(AF.type)"},
                {"source_table", "create_time", "sink_table", "begin_time", "FINAL(LAST(BF.create_time, 0))"},
                {"source_table", "create_time", "sink_table", "end_time", "FINAL(LAST(AF.create_time, 0))"},
        };

        // 调用 analyzeLineage 方法验证结果
        analyzeLineage(sql, expectedArray);
    }
}
