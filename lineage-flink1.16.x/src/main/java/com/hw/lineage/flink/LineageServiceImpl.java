package com.hw.lineage.flink;

import com.hw.lineage.common.enums.TableKind;
import com.hw.lineage.common.model.*;
import com.hw.lineage.common.service.LineageService;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.base.CaseFormat;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.functions.*;
import org.apache.flink.table.operations.CreateTableASOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.metadata.FlinkDefaultRelMetadataProvider;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import static com.hw.lineage.common.util.Constant.DELIMITER;
import static java.util.Objects.requireNonNull;

/**
 * @description: LineageContext - 实现 LineageService 接口，用于分析 SQL 的字段血缘关系和自定义函数
 */
public class LineageServiceImpl implements LineageService {

    // 日志记录器，用于打印日志信息
    private static final Logger LOG = LoggerFactory.getLogger(LineageServiceImpl.class);

    // 查询建表 SQL 的模板
    private static final String SHOW_CREATE_TABLE_SQL = "SHOW CREATE TABLE %s.`%s`.%s";

    // 查询视图 SQL 的模板
    private static final String SHOW_CREATE_VIEW_SQL = "SHOW CREATE VIEW %s.`%s`.%s";

    // 映射不同类型的函数到其后缀，例如 UDF、UDTF 等
    private static final Map<String, String> FUNCTION_SUFFIX_MAP = ImmutableMap.of(
            ScalarFunction.class.getName(), "udf", // 标量函数后缀
            TableFunction.class.getName(), "udtf", // 表函数后缀
            AggregateFunction.class.getName(), "udaf", // 聚合函数后缀
            TableAggregateFunction.class.getName(), "udtaf" // 表聚合函数后缀
    );

    // Flink TableEnvironment 实现，用于执行 SQL 和管理表
    private final TableEnvironmentImpl tableEnv;

    /**
     * 构造方法，初始化 TableEnvironmentImpl
     */
    public LineageServiceImpl() {
        // 配置动态表选项
        Configuration configuration = new Configuration();
        configuration.setBoolean("table.dynamic-table-options.enabled", true);

        // 创建本地流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);

        // 设置为流模式
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        // 创建流式 TableEnvironment 实例
        this.tableEnv = (TableEnvironmentImpl) StreamTableEnvironment.create(env, settings);
    }

    /**
     * 使用指定的 Catalog（目录）
     *
     * @param catalog AbstractCatalog 实例
     */
    public void useCatalog(AbstractCatalog catalog) {
        // 如果指定的 Catalog 尚未注册，则进行注册
        if (!tableEnv.getCatalog(catalog.getName()).isPresent()) {
            tableEnv.registerCatalog(catalog.getName(), catalog);
        }
        // 切换到指定的 Catalog
        tableEnv.useCatalog(catalog.getName());
    }

    /**
     * 执行单条 SQL 语句
     *
     * @param singleSql SQL 语句
     */
    @Override
    public void execute(String singleSql) {
        executeSql(singleSql);
    }

    /**
     * 分析 SQL 中的函数，提取自定义函数信息
     *
     * @param singleSql SQL 语句
     * @return 包含函数信息的集合
     */
    @Override
    public Set<FunctionResult> analyzeFunction(String singleSql) {
        // 打印日志，记录需要分析的 SQL
        LOG.info("Analyze function Sql: \n {}", singleSql);

        // 获取 SQL 解析器
        ParserImpl parser = (ParserImpl) tableEnv.getParser();

        // 解析 SQL，生成抽象语法树 (AST)
        SqlNode sqlNode = parser.parseSql(singleSql);

        // 验证 SQL 的合法性，返回经过验证的 AST
        SqlNode validated = parser.validate(sqlNode);

        // 创建函数访问器，用于遍历 AST 并查找所有函数
        FunctionVisitor visitor = new FunctionVisitor();
        validated.accept(visitor);

        // 获取函数列表
        List<UnresolvedIdentifier> fullFunctionList = visitor.getFunctionList();

        // 创建结果集，用于存储自定义函数信息
        Set<FunctionResult> resultSet = new HashSet<>();
        for (UnresolvedIdentifier unresolvedIdentifier : fullFunctionList) {
            // 查找函数并获取其标识符信息
            getFunctionCatalog()
                    .lookupFunction(unresolvedIdentifier)
                    .flatMap(ContextResolvedFunction::getIdentifier)
                    // 忽略内置函数（标识符为空）
                    .flatMap(FunctionIdentifier::getIdentifier)
                    .ifPresent(identifier -> {
                        // 构造函数结果对象，保存函数的 Catalog、数据库和函数名
                        FunctionResult functionResult = new FunctionResult()
                                .setCatalogName(identifier.getCatalogName())
                                .setDatabase(identifier.getDatabaseName())
                                .setFunctionName(identifier.getObjectName());
                        // 打印调试信息
                        LOG.debug("analyzed function: {}", functionResult);
                        // 添加到结果集中
                        resultSet.add(functionResult);
                    });
        }
        return resultSet;
    }


    /**
     * 获取 FunctionCatalog 对象
     *
     * @return FunctionCatalog 对象，用于函数相关操作
     */
    private FunctionCatalog getFunctionCatalog() {
        // 从 TableEnvironment 中获取 PlannerBase，然后从中获取 FunctionCatalog
        PlannerBase planner = (PlannerBase) tableEnv.getPlanner();
        return planner.getFlinkContext().getFunctionCatalog();
    }

    /**
     * 执行单条 SQL 语句
     *
     * @param singleSql SQL 语句
     * @return TableResult 包含 SQL 执行的结果
     */
    private TableResult executeSql(String singleSql) {
        // 打印日志，记录执行的 SQL
        LOG.info("Execute SQL: {}", singleSql);
        return tableEnv.executeSql(singleSql);
    }

    /**
     * 分析 SQL 的字段血缘关系
     *
     * @param singleSql SQL 语句
     * @return 字段血缘结果的列表
     */
    @Override
    public List<LineageResult> analyzeLineage(String singleSql) {
        /*
         * 由于 TableEnvironment 不是线程安全的，因此需要添加以下设置。
         * 否则，在 org.apache.calcite.rel.metadata.RelMetadataQuery 的构造方法中可能会出现 NullPointerException。
         * 相关问题详见：
         * http://apache-flink.370.s1.nabble.com/flink1-11-0-sqlQuery-NullPointException-td5466.html
         */
        RelMetadataQueryBase.THREAD_PROVIDERS
                .set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE()));

        // 打印日志，记录要分析的 SQL
        LOG.info("Analyze lineage Sql: \n {}", singleSql);

        // 第一步：生成原始的 RelNode 树
        Tuple2<String, RelNode> parsed = parseStatement(singleSql);
        String sinkTable = parsed.getField(0); // 获取目标表名称
        RelNode oriRelNode = parsed.getField(1); // 获取原始 RelNode

        if (LOG.isDebugEnabled()) {
            // 打印原始 RelNode 的详细信息
            LOG.debug("Original RelNode: \n {}", oriRelNode.explain());
        }

        // 第二步：基于 RelMetadataQuery 构建字段血缘结果
        return buildFiledLineageResult(sinkTable, oriRelNode);
    }

    /**
     * 解析 SQL，生成目标表名称和对应的 RelNode
     *
     * @param singleSql SQL 语句
     * @return 包含目标表名称和 RelNode 的元组
     */
    private Tuple2<String, RelNode> parseStatement(String singleSql) {
        // 解析、验证并转换 SQL
        Operation operation = parseValidateConvert(singleSql);

        // 预处理 CTAS（CREATE TABLE AS SELECT）操作
        operation = prePocessCreateTableAsOperation(operation);

        // 处理插入操作
        if (operation instanceof SinkModifyOperation) {
            SinkModifyOperation sinkOperation = (SinkModifyOperation) operation;

            // 获取子查询的 RelNode
            PlannerQueryOperation queryOperation = (PlannerQueryOperation) sinkOperation.getChild();
            RelNode relNode = queryOperation.getCalciteTree();

            // 返回目标表名称和原始 RelNode
            return new Tuple2<>(
                    sinkOperation.getContextResolvedTable().getIdentifier().asSummaryString(),
                    relNode);
        } else {
            // 如果不是插入操作，抛出异常
            throw new TableException("Only insert is supported now.");
        }
    }

    /**
     * 解析、验证并转换 SQL
     *
     * @param singleSql SQL 语句
     * @return 解析后的 Operation 对象
     */
    private Operation parseValidateConvert(String singleSql) {
        /*
         * 由于 TableEnvironment 不是线程安全的，因此需要添加以下设置。
         * 否则，在 org.apache.calcite.rel.metadata.RelMetadataQuery 的构造方法中可能会出现 NullPointerException。
         * 相关问题详见：
         * http://apache-flink.370.s1.nabble.com/flink1-11-0-sqlQuery-NullPointException-td5466.html
         */
        RelMetadataQueryBase.THREAD_PROVIDERS
                .set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE()));

        // 使用 TableEnvironment 的解析器解析 SQL
        List<Operation> operations = tableEnv.getParser().parse(singleSql);
        if (operations.size() != 1) {
            // 如果解析出的操作数不为 1，抛出异常
            throw new TableException(
                    "Unsupported SQL query! only accepts a single SQL statement.");
        }
        return operations.get(0); // 返回第一个操作对象
    }

    /**
     * 预处理 CREATE TABLE AS SELECT 操作
     *
     * @param operation 待处理的操作
     * @return 修改后的操作
     */
    private Operation prePocessCreateTableAsOperation(Operation operation) {
        if (operation instanceof CreateTableASOperation) {
            // 如果操作是 CTAS（CREATE TABLE AS SELECT）
            CreateTableASOperation ctasOperation = (CreateTableASOperation) operation;
            CreateTableOperation createTableOperation = ctasOperation.getCreateTableOperation();

            // 判断是临时表还是正式表，并注册表
            if (createTableOperation.isTemporary()) {
                tableEnv.getCatalogManager().createTemporaryTable(
                        createTableOperation.getCatalogTable(),
                        createTableOperation.getTableIdentifier(),
                        createTableOperation.isIgnoreIfExists());
            } else {
                tableEnv.getCatalogManager().createTable(
                        createTableOperation.getCatalogTable(),
                        createTableOperation.getTableIdentifier(),
                        createTableOperation.isIgnoreIfExists());
            }

            // 返回转换后的插入操作
            return ctasOperation.toSinkModifyOperation(tableEnv.getCatalogManager());
        }
        return operation; // 如果不是 CTAS 操作，直接返回原操作
    }


    /**
     * 构建字段血缘结果
     *
     * @param sinkTable 目标表名称
     * @param optRelNode 优化后的 RelNode
     * @return 字段血缘结果的列表
     */
    private List<LineageResult> buildFiledLineageResult(String sinkTable, RelNode optRelNode) {
        // 获取目标表的字段列表
        List<String> targetColumnList = tableEnv.from(sinkTable)
                .getResolvedSchema()
                .getColumnNames();

        // 检查查询结果字段和目标表字段数量是否一致
        validateSchema(sinkTable, optRelNode, targetColumnList);

        // 获取 RelMetadataQuery，用于查询字段来源信息
        RelMetadataQuery metadataQuery = optRelNode.getCluster().getMetadataQuery();
        List<LineageResult> resultList = new ArrayList<>();

        // 遍历目标表的字段
        for (int index = 0; index < targetColumnList.size(); index++) {
            String targetColumn = targetColumnList.get(index);

            LOG.debug("**********************************************************");
            LOG.debug("Target table: {}", sinkTable);
            LOG.debug("Target column: {}", targetColumn);

            // 查询字段来源信息
            Set<RelColumnOrigin> relColumnOriginSet = metadataQuery.getColumnOrigins(optRelNode, index);

            if (CollectionUtils.isNotEmpty(relColumnOriginSet)) {
                for (RelColumnOrigin rco : relColumnOriginSet) {
                    // 获取字段来源的表信息
                    RelOptTable table = rco.getOriginTable();
                    String sourceTable = String.join(DELIMITER, table.getQualifiedName());

                    // 获取字段来源的列名
                    int ordinal = rco.getOriginColumnOrdinal();
                    List<String> fieldNames =
                            ((TableSourceTable) table).contextResolvedTable().getResolvedSchema().getColumnNames();
                    String sourceColumn = fieldNames.get(ordinal);

                    LOG.debug("----------------------------------------------------------");
                    LOG.debug("Source table: {}", sourceTable);
                    LOG.debug("Source column: {}", sourceColumn);

                    // 如果存在字段的转换信息，则打印
                    if (StringUtils.isNotEmpty(rco.getTransform())) {
                        LOG.debug("transform: {}", rco.getTransform());
                    }

                    // 添加血缘关系记录到结果列表
                    resultList.add(
                            new LineageResult(sourceTable, sourceColumn, sinkTable, targetColumn, rco.getTransform()));
                }
            }
        }
        return resultList;
    }

    /**
     * 验证查询结果字段和目标表字段是否匹配
     *
     * @param sinkTable 目标表名称
     * @param relNode 查询的 RelNode
     * @param sinkFieldList 目标表字段列表
     */
    private void validateSchema(String sinkTable, RelNode relNode, List<String> sinkFieldList) {
        // 获取查询结果的字段列表
        List<String> queryFieldList = relNode.getRowType().getFieldNames();
        // 检查字段数量是否一致
        if (queryFieldList.size() != sinkFieldList.size()) {
            throw new ValidationException(
                    String.format(
                            "Column types of query result and sink for %s do not match.\n"
                                    + "Query schema: %s\n"
                                    + "Sink schema:  %s",
                            sinkTable, queryFieldList, sinkFieldList));
        }
    }

    /**
     * 解析并验证 SQL
     *
     * @param singleSql SQL 语句
     */
    @Override
    public void parseValidate(String singleSql) {
        LOG.info("Parse validate Sql: \n {}", singleSql);
        parseValidateConvert(singleSql);
    }

    /**
     * 从 JAR 文件中解析函数信息
     *
     * @param file JAR 文件
     * @return 函数信息的列表
     * @throws IOException 读取 JAR 文件异常
     * @throws ClassNotFoundException 类加载异常
     */
    @Override
    public List<FunctionInfo> parseFunction(File file) throws IOException, ClassNotFoundException {
        LOG.info("starting parse function from jar {}", file.getPath());
        List<FunctionInfo> resultList = new ArrayList<>();
        URL url = file.toURI().toURL();

        // 使用 URLClassLoader 加载 JAR 文件
        try (URLClassLoader classLoader = new URLClassLoader(new URL[]{url}, getClass().getClassLoader());
             // 打开 JAR 文件以读取其内容
             JarFile jarFile = new JarFile(file)) {
            // 获取 JAR 文件中的所有条目（包括类文件、资源文件等）
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                // 获取当前条目的名称
                String entryName = entries.nextElement().getName();
                // 判断当前条目是否是类文件（以 .class 结尾）
                if (entryName.endsWith(".class")) {
                    // 将类文件路径转换为 Java 类名格式
                    // 例如：将 "com/example/MyClass.class" 转换为 "com.example.MyClass"
                    String className = entryName.replace("/", ".").substring(0, entryName.length() - 6);

                    // 过滤掉不相关的类，例如 commons-compress.jar 中的类或 Flink 的内置函数类
                    if (!className.startsWith("org.apache.commons.compress.harmony")
                            && !className.startsWith("org.apache.flink.table.functions")) {
                        // 使用类加载器加载当前类
                        Class<?> clazz = classLoader.loadClass(className);
                        // 检查类的父类是否不为空，并且父类是否在 FUNCTION_SUFFIX_MAP 中
                        // FUNCTION_SUFFIX_MAP 映射了用户自定义函数的类型，例如 ScalarFunction, TableFunction 等
                        if (clazz.getSuperclass() != null
                                && FUNCTION_SUFFIX_MAP.containsKey(clazz.getSuperclass().getName())) {
                            // 如果类是用户自定义函数，则解析该类并将结果添加到结果列表中
                            resultList.add(parseUserDefinedFunction(clazz));
                        }
                    }
                }
            }
        }


        LOG.info("finished parse function from jar {}, resultList: {}", file.getPath(), resultList);
        return resultList;
    }


    /**
     * 解析用户自定义函数的信息
     *
     * @param clazz 用户自定义函数的类
     * @return FunctionInfo 对象，包含函数的名称、调用格式和描述等信息
     */
    public FunctionInfo parseUserDefinedFunction(Class<?> clazz) {
        // 获取函数的完整类名
        String functionClass = clazz.getName();
        // 提取类名
        String className = searchClassName(functionClass);
        // 将类名从驼峰格式转换为下划线格式
        String functionName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, className);
        // 替换函数后缀，例如将 "function" 替换为 "udf", "udtf" 等
        functionName = functionName.replace("function", FUNCTION_SUFFIX_MAP.get(clazz.getSuperclass().getName()));

        // 查找类中名为 "eval" 的方法，用户自定义函数通常通过 "eval" 方法实现  TODO 改造成通用方法
        Optional<Method> methodOptional = Arrays.stream(clazz.getDeclaredMethods())
                .filter(e -> "eval".equals(e.getName()))
                .findFirst();

        // 创建 FunctionInfo 对象
        FunctionInfo result = new FunctionInfo();
        result.setClassName(functionClass); // 设置类名
        result.setFunctionName(functionName); // 设置函数名

        // 如果找到 "eval" 方法
        if (methodOptional.isPresent()) {
            Method method = methodOptional.get();
            AtomicInteger atomicInteger = new AtomicInteger(1);
            // 获取方法参数列表并生成调用格式
            String parameters = Arrays.stream(method.getParameters())
                    .map(parameter -> searchClassName(parameter.getType().getName() + atomicInteger.getAndIncrement()))
                    .collect(Collectors.joining(","));

            // 设置函数的调用格式，例如 "my_udf(param1, param2)"
            result.setInvocation(String.format("%s(%s)", functionName, parameters));

            // 使用函数的返回类型作为描述
            result.setDescr(buildFunctionReturnType(clazz, method));
        }
        return result;
    }

    /**
     * 构建函数的返回类型描述
     *
     * @param clazz 函数的类
     * @param method 函数的方法
     * @return 返回类型的描述
     */
    private String buildFunctionReturnType(Class<?> clazz, Method method) {
        // 如果是 TableFunction 且带有 FunctionHint 注解，则使用注解中的返回类型
        if (clazz.getSuperclass().isAssignableFrom(TableFunction.class)
                && clazz.isAnnotationPresent(FunctionHint.class)) {
            return "return " + clazz.getAnnotation(FunctionHint.class).output().value();
        }
        // 否则，直接使用方法的返回类型
        return "return " + searchClassName(method.getReturnType().getName());
    }

    /**
     * 提取类名或字段名
     *
     * @param value 全限定名或普通名
     * @return 提取后的类名或字段名
     */
    private String searchClassName(String value) {
        return value.contains(".") ? value.substring(value.lastIndexOf(".") + 1) : value;
    }

    /**
     * 根据 Catalog 名称获取 Catalog 对象
     *
     * @param catalogName Catalog 名称
     * @return Catalog 对象
     * @throws ValidationException 如果指定的 Catalog 不存在
     */
    private Catalog getCatalog(String catalogName) {
        return tableEnv.getCatalog(catalogName)
                .orElseThrow(() -> new ValidationException(String.format("Catalog %s does not exist", catalogName)));
    }

    /**
     * 列出指定 Catalog 中的数据库列表
     *
     * @param catalogName Catalog 名称
     * @return 数据库名称列表
     */
    @Override
    public List<String> listDatabases(String catalogName) {
        return getCatalog(catalogName).listDatabases();
    }

    /**
     * 列出指定 Catalog 和数据库中的表列表
     *
     * @param catalogName Catalog 名称
     * @param database 数据库名称
     * @return 表名称列表
     * @throws Exception 如果发生错误
     */
    @Override
    public List<String> listTables(String catalogName, String database) throws Exception {
        return getCatalog(catalogName).listTables(database);
    }

    /**
     * 列出指定 Catalog 和数据库中的视图列表
     *
     * @param catalogName Catalog 名称
     * @param database 数据库名称
     * @return 视图名称列表
     * @throws Exception 如果发生错误
     */
    @Override
    public List<String> listViews(String catalogName, String database) throws Exception {
        return getCatalog(catalogName).listViews(database);
    }

    /**
     * 获取指定表的信息
     *
     * @param catalogName Catalog 名称
     * @param database 数据库名称
     * @param tableName 表名称
     * @return TableInfo 对象，包含表的元数据信息
     * @throws Exception 如果发生错误
     */
    @Override
    public TableInfo getTable(String catalogName, String database, String tableName) throws Exception {
        // 创建表的 ObjectPath
        ObjectPath objectPath = new ObjectPath(database, tableName);
        // 从 Catalog 中获取表的元信息
        CatalogBaseTable table = getCatalog(catalogName).getTable(objectPath);
        // 获取表的 Schema
        Schema schema = table.getUnresolvedSchema();
        LOG.info("table.schema: {}", schema);

        // 获取主键列表
        List<String> primaryKeyList = new ArrayList<>();
        schema.getPrimaryKey()
                .ifPresent(pk -> primaryKeyList.addAll(pk.getColumnNames()));

        // 获取水印信息
        Map<String, String> watermarkMap = schema.getWatermarkSpecs()
                .stream()
                .collect(Collectors.toMap(Schema.UnresolvedWatermarkSpec::getColumnName,
                        entry -> entry.getWatermarkExpression().toString()));

        // 构建列信息列表
        List<ColumnInfo> columnList = schema.getColumns()
                .stream()
                .map(column -> new ColumnInfo()
                        .setColumnName(column.getName())
                        .setColumnType(processColumnType(column)) // 处理字段类型
                        .setComment(column.getComment().orElse("")) // 获取字段注释
                        .setPrimaryKey(primaryKeyList.contains(column.getName())) // 判断是否是主键
                        .setWatermark(watermarkMap.getOrDefault(column.getName(), ""))) // 设置水印表达式
                .collect(Collectors.toList());

        // 构建 TableInfo 对象并返回
        return new TableInfo()
                .setTableName(tableName)
                .setTableKind(TableKind.valueOf(table.getTableKind().name()))
                .setComment(table.getComment())
                .setColumnList(columnList)
                .setPropertiesMap(table.getOptions());
    }


    /**
     * 处理列的类型信息，根据列的具体类型（物理列、计算列、元数据列）返回相应的描述
     *
     * @param column Schema.UnresolvedColumn 列信息
     * @return 字段类型的描述字符串
     */
    private String processColumnType(Schema.UnresolvedColumn column) {
        // 如果是计算列
        if (column instanceof Schema.UnresolvedComputedColumn) {
            // 返回计算列的表达式摘要字符串
            return ((Schema.UnresolvedComputedColumn) column)
                    .getExpression()
                    .asSummaryString();
        }
        // 如果是物理列
        else if (column instanceof Schema.UnresolvedPhysicalColumn) {
            // 获取物理列的数据类型字符串
            return ((Schema.UnresolvedPhysicalColumn) column).getDataType()
                    .toString()
                    // 去掉 "NOT NULL" 关键字并去除多余空格
                    .replace("NOT NULL", "")
                    .trim();
        }
        // 如果是元数据列
        else if (column instanceof Schema.UnresolvedMetadataColumn) {
            // 返回元数据列的数据类型字符串
            return ((Schema.UnresolvedMetadataColumn) column).getDataType().toString();
        }
        // 其他未知类型返回 "unknown"
        return "unknown";
    }

    /**
     * 删除指定的表
     *
     * @param catalogName Catalog 名称
     * @param database 数据库名称
     * @param tableName 表名称
     * @throws Exception 如果操作失败
     */
    @Override
    public void dropTable(String catalogName, String database, String tableName) throws Exception {
        // 构建表的 ObjectPath
        ObjectPath objectPath = new ObjectPath(database, tableName);
        // 调用 Catalog 的 dropTable 方法删除表，不忽略不存在的表
        getCatalog(catalogName).dropTable(objectPath, false);
    }

    /**
     * 获取指定表的 DDL（数据定义语言）语句
     *
     * @param catalogName Catalog 名称
     * @param database 数据库名称
     * @param tableName 表名称
     * @return 表的 DDL 语句
     * @throws Exception 如果操作失败
     */
    @Override
    public String getTableDdl(String catalogName, String database, String tableName) throws Exception {
        // 获取表的元信息
        TableInfo tableInfo = getTable(catalogName, database, tableName);

        // 根据表的类型（TABLE 或 VIEW）选择对应的 SHOW CREATE 语句
        String showCreateSql = tableInfo.getTableKind().equals(TableKind.TABLE)
                ? String.format(SHOW_CREATE_TABLE_SQL, catalogName, database, tableName) // SHOW CREATE TABLE
                : String.format(SHOW_CREATE_VIEW_SQL, catalogName, database, tableName); // SHOW CREATE VIEW

        // 执行 SHOW CREATE 语句
        TableResult tableResult = executeSql(showCreateSql);

        // 获取结果中生成的 DDL 语句
        String tableDdl = requireNonNull(tableResult.collect().next().getField(0)).toString();

        // 去掉 Catalog 和 Database 的全限定名前缀，简化表名称
        return tableDdl.replace(String.format("`%s`.`%s`.", catalogName, database), "");
    }

    /**
     * 获取当前使用的 Catalog 名称
     *
     * @return 当前 Catalog 名称
     */
    public String getCurrentCatalog() {
        // 调用 TableEnvironment 的方法获取当前 Catalog 名称
        return tableEnv.getCurrentCatalog();
    }

}
