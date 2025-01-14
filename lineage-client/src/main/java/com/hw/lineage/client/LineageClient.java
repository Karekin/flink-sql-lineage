package com.hw.lineage.client;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.hw.lineage.common.exception.LineageException;
import com.hw.lineage.common.model.FunctionInfo;
import com.hw.lineage.common.model.FunctionResult;
import com.hw.lineage.common.model.LineageResult;
import com.hw.lineage.common.model.TableInfo;
import com.hw.lineage.common.service.LineageService;
import com.hw.lineage.common.util.Preconditions;
import com.hw.lineage.loader.classloading.TemporaryClassLoaderContext;
import com.hw.lineage.loader.plugin.PluginDescriptor;
import com.hw.lineage.loader.plugin.finder.DirectoryBasedPluginFinder;
import com.hw.lineage.loader.plugin.finder.PluginFinder;
import com.hw.lineage.loader.plugin.manager.DefaultPluginManager;
import com.hw.lineage.loader.plugin.manager.PluginManager;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static com.hw.lineage.common.util.Preconditions.checkArgument;

/**
 * @description: LineageClient（血缘客户端）
 * 提供对SQL血缘分析的功能，包括SQL解析、验证、执行，以及管理Catalog、Database和自定义函数的操作。
 * 支持通过插件机制动态加载不同实现。
 */
public class LineageClient {

    /**
     * Catalog（数据目录相关操作的SQL模板）
     */
    private static final String CREATE_CATALOG_SQL = "CREATE CATALOG %s WITH (%s)"; // 创建数据目录
    private static final String USE_CATALOG_SQL = "USE CATALOG %s";                 // 使用指定数据目录
    private static final String DROP_CATALOG_SQL = "DROP CATALOG IF EXISTS %s";     // 删除数据目录（如果存在）

    /**
     * Database（数据库相关操作的SQL模板）
     */
    private static final String CREATE_DATABASE_SQL = "CREATE DATABASE IF NOT EXISTS %s.`%s` COMMENT '%s'"; // 创建数据库
    private static final String USE_DATABASE_SQL = "USE %s.`%s`";                                           // 使用指定数据库
    private static final String DROP_DATABASE_SQL = "DROP DATABASE IF EXISTS %s.`%s`";                     // 删除数据库（如果存在）

    /**
     * Function（自定义函数相关操作的SQL模板）
     */
    private static final String CREATE_FUNCTION_SQL = "CREATE FUNCTION IF NOT EXISTS %s.`%s`.%s AS '%s' USING JAR '%s'"; // 创建自定义函数
    private static final String DROP_FUNCTION_SQL = "DROP FUNCTION IF EXISTS %s.`%s`.%s";                               // 删除自定义函数

    /**
     * 存储加载的插件服务，键为插件代码，值为插件对应的 LineageService 实现。
     */
    private final Map<String, LineageService> lineageServiceMap;

    /**
     * 构造函数，初始化 LineageClient 并加载指定路径的插件。
     *
     * @param path 插件目录路径
     */
    public LineageClient(String path) {
        // 加载插件并生成插件服务映射
        Map<String, Iterator<LineageService>> pluginIteratorMap = loadPlugins(path);

        // 确保每个插件代码只有一个 LineageService 实现
        this.lineageServiceMap = pluginIteratorMap.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    List<LineageService> lineageServiceList = Lists.newArrayList(entry.getValue());
                    checkArgument(lineageServiceList.size() == 1,
                            "%s plugin no implementation of LineageService or greater than 1", entry.getKey());
                    return lineageServiceList.get(0);
                }));
    }

    /**
     * 加载插件，返回插件代码与对应服务迭代器的映射。
     *
     * @param path 插件目录路径
     * @return 插件代码与服务迭代器的映射
     */
    private Map<String, Iterator<LineageService>> loadPlugins(String path) {
        File pluginRootFolder = new File(path);
        if (!pluginRootFolder.exists()) {
            // 如果目录不存在，为测试提供兼容路径
            pluginRootFolder = new File("../../" + path);
        }
        Path pluginRootFolderPath = pluginRootFolder.toPath();

        // 创建插件描述符工厂
        PluginFinder descriptorsFactory = new DirectoryBasedPluginFinder(pluginRootFolderPath);
        Collection<PluginDescriptor> descriptors;
        try {
            descriptors = descriptorsFactory.findPlugins();
        } catch (IOException e) {
            throw new LineageException("Exception when trying to initialize plugin system.", e);
        }

        // 使用默认插件管理器加载插件
        String[] parentPatterns = {"com.hw.lineage.common"};
        PluginManager pluginManager =
                new DefaultPluginManager(descriptors, LineageService.class.getClassLoader(), parentPatterns);

        return pluginManager.load(LineageService.class);
    }

    /**
     * 根据插件代码获取对应的 LineageService 实现。
     *
     * @param pluginCode 插件代码
     * @return 对应的 LineageService 实现
     */
    private LineageService getLineageService(String pluginCode) {
        LineageService lineageService = lineageServiceMap.get(pluginCode);
        Preconditions.checkNotNull(lineageService, "This plugin %s is not supported.", pluginCode);
        return lineageService;
    }

    /**
     * 分析输入 SQL 的字段级血缘关系。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     * @param database    数据库名称
     * @param singleSql   单条SQL语句
     * @return 字段级血缘分析结果的列表
     */
    public List<LineageResult> analyzeLineage(String pluginCode, String catalogName, String database,
                                              String singleSql) {
        LineageService service = getLineageService(pluginCode);
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(service.getClassLoader())) {
            service.execute(String.format(USE_DATABASE_SQL, catalogName, database));
            return service.analyzeLineage(singleSql);
        }
    }

    /**
     * 分析 SQL 中使用的自定义函数。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     * @param database    数据库名称
     * @param singleSql   单条SQL语句
     * @return 使用的自定义函数分析结果的集合
     */
    public Set<FunctionResult> analyzeFunction(String pluginCode, String catalogName, String database,
                                               String singleSql) {
        LineageService service = getLineageService(pluginCode);
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(service.getClassLoader())) {
            service.execute(String.format(USE_DATABASE_SQL, catalogName, database));
            return service.analyzeFunction(singleSql);
        }
    }

    /**
     * 对 SQL 进行解析和验证。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     * @param database    数据库名称
     * @param singleSql   单条SQL语句
     */
    public void parseValidate(String pluginCode, String catalogName, String database, String singleSql) {
        LineageService service = getLineageService(pluginCode);
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(service.getClassLoader())) {
            service.execute(String.format(USE_DATABASE_SQL, catalogName, database));
            service.parseValidate(singleSql);
        }
    }

    /**
     * 执行单条 SQL 语句。
     *
     * @param pluginCode 插件代码
     * @param singleSql  单条SQL语句
     */
    public void execute(String pluginCode, String singleSql) {
        LineageService service = getLineageService(pluginCode);
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(service.getClassLoader())) {
            service.execute(singleSql);
        }
    }

    /**
     * 执行单条 SQL 语句。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     * @param database    数据库名称
     * @param singleSql   单条 SQL 语句
     */
    public void execute(String pluginCode, String catalogName, String database, String singleSql) {
        LineageService service = getLineageService(pluginCode);
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(service.getClassLoader())) {
            // 切换到指定的数据库
            service.execute(String.format(USE_DATABASE_SQL, catalogName, database));
            // 执行 SQL 语句
            service.execute(singleSql);
        }
    }

    /**
     * 从文件中解析函数信息。
     *
     * @param pluginCode 插件代码
     * @param file       包含函数的文件
     * @return 函数信息列表
     * @throws IOException            如果文件读取失败
     * @throws ClassNotFoundException 如果加载类失败
     */
    public List<FunctionInfo> parseFunction(String pluginCode, File file) throws IOException, ClassNotFoundException {
        LineageService service = getLineageService(pluginCode);
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(service.getClassLoader())) {
            return service.parseFunction(file);
        }
    }

    /**
     * 创建数据目录。
     *
     * @param pluginCode    插件代码
     * @param catalogName   数据目录名称
     * @param propertiesMap 数据目录属性的键值对
     */
    public void createCatalog(String pluginCode, String catalogName, Map<String, String> propertiesMap) {
        String properties = propertiesMap.entrySet()
                .stream()
                .map(entry -> String.format("'%s'='%s'", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(","));
        execute(pluginCode, String.format(CREATE_CATALOG_SQL, catalogName, properties));
    }

    /**
     * 切换到指定数据目录。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     */
    public void useCatalog(String pluginCode, String catalogName) {
        execute(pluginCode, String.format(USE_CATALOG_SQL, catalogName));
    }

    /**
     * 切换到指定数据库。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     * @param database    数据库名称
     */
    public void useDatabase(String pluginCode, String catalogName, String database) {
        execute(pluginCode, String.format(USE_DATABASE_SQL, catalogName, database));
    }

    /**
     * 删除指定数据目录。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     */
    public void deleteCatalog(String pluginCode, String catalogName) {
        execute(pluginCode, String.format(DROP_CATALOG_SQL, catalogName));
    }

    /**
     * 创建数据库。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     * @param database    数据库名称
     * @param comment     数据库注释
     */
    public void createDatabase(String pluginCode, String catalogName, String database, String comment) {
        comment = Strings.nullToEmpty(comment);
        execute(pluginCode, String.format(CREATE_DATABASE_SQL, catalogName, database, comment));
    }

    /**
     * 删除数据库。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     * @param database    数据库名称
     */
    public void deleteDatabase(String pluginCode, String catalogName, String database) {
        execute(pluginCode, String.format(DROP_DATABASE_SQL, catalogName, database));
    }

    /**
     * 创建自定义函数。
     *
     * @param pluginCode   插件代码
     * @param catalogName  数据目录名称
     * @param database     数据库名称
     * @param functionName 函数名称
     * @param className    函数对应的类名
     * @param functionPath 函数实现的路径
     */
    public void createFunction(String pluginCode, String catalogName, String database, String functionName,
                               String className, String functionPath) {
        execute(pluginCode,
                String.format(CREATE_FUNCTION_SQL, catalogName, database, functionName, className, functionPath));
    }

    /**
     * 删除自定义函数。
     *
     * @param pluginCode   插件代码
     * @param catalogName  数据目录名称
     * @param database     数据库名称
     * @param functionName 函数名称
     */
    public void deleteFunction(String pluginCode, String catalogName, String database, String functionName) {
        execute(pluginCode, String.format(DROP_FUNCTION_SQL, catalogName, database, functionName));
    }

    /**
     * 删除表。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     * @param database    数据库名称
     * @param tableName   表名称
     * @throws Exception 如果操作失败
     */
    public void deleteTable(String pluginCode, String catalogName, String database, String tableName) throws Exception {
        LineageService service = getLineageService(pluginCode);
        service.dropTable(catalogName, database, tableName);
    }

    /**
     * 获取指定数据目录下的所有数据库名称。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     * @return 数据库名称列表
     */
    public List<String> listDatabases(String pluginCode, String catalogName) {
        LineageService service = getLineageService(pluginCode);
        return service.listDatabases(catalogName);
    }

    /**
     * 获取指定数据库下的所有表和视图名称。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     * @param database    数据库名称
     * @return 表和视图名称列表
     * @throws Exception 如果操作失败
     */
    public List<String> listTables(String pluginCode, String catalogName, String database) throws Exception {
        LineageService service = getLineageService(pluginCode);
        return service.listTables(catalogName, database);
    }

    /**
     * 获取指定数据库下的所有视图名称。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     * @param database    数据库名称
     * @return 视图名称列表
     * @throws Exception 如果操作失败
     */
    public List<String> listViews(String pluginCode, String catalogName, String database) throws Exception {
        LineageService service = getLineageService(pluginCode);
        return service.listViews(catalogName, database);
    }

    /**
     * 获取注册表的信息。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     * @param database    数据库名称
     * @param tableName   表名称
     * @return 表信息对象
     * @throws Exception 如果操作失败
     */
    public TableInfo getTable(String pluginCode, String catalogName, String database, String tableName) throws Exception {
        LineageService service = getLineageService(pluginCode);
        return service.getTable(catalogName, database, tableName);
    }

    /**
     * 获取指定表的DDL（数据定义语言）。
     *
     * @param pluginCode  插件代码
     * @param catalogName 数据目录名称
     * @param database    数据库名称
     * @param tableName   表名称
     * @return 表的DDL字符串
     * @throws Exception 如果操作失败
     */
    public String getTableDdl(String pluginCode, String catalogName, String database, String tableName) throws Exception {
        LineageService service = getLineageService(pluginCode);
        return service.getTableDdl(catalogName, database, tableName);
    }

}
