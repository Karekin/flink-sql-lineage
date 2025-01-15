package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Hive 连接器的测试工具类。
 *
 * @description: HiveTestUtils 提供了与 HiveCatalog 相关的辅助方法，
 * 包括创建 HiveCatalog、生成 Hive 配置和清理临时文件夹。
 */
public class HiveTestUtils {

    // 定义 Hive 仓库 URI 格式，用于本地测试 Hive Metastore
    private static final String HIVE_WAREHOUSE_URI_FORMAT = "jdbc:derby:;databaseName=%s;create=true";

    // 临时文件夹对象，用于创建 Hive 仓库的临时目录
    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    /**
     * 创建一个 HiveCatalog 实例，用于管理 Hive 表和元数据。
     *
     * @param catalogName    Catalog 名称
     * @param defaultDatabase 默认数据库名称
     * @param hiveVersion    Hive 的版本号
     * @return 初始化完成的 HiveCatalog 实例
     */
    public static HiveCatalog createHiveCatalog(String catalogName, String defaultDatabase, String hiveVersion) {
        return new HiveCatalog(
                catalogName,                 // Catalog 名称
                defaultDatabase,             // 默认数据库
                createHiveConf(),            // Hive 配置
                hiveVersion,                 // Hive 版本号
                true);                       // 是否允许创建缺失的数据库
    }

    /**
     * 创建一个 HiveConf 对象，包含用于测试的 Hive 配置信息。
     *
     * @return 初始化完成的 HiveConf 对象
     */
    public static HiveConf createHiveConf() {
        ClassLoader classLoader = HiveTestUtils.class.getClassLoader();

        try {
            // 创建临时文件夹，用于存放测试 Hive 的仓库数据
            TEMPORARY_FOLDER.create();
            String warehouseDir = TEMPORARY_FOLDER.newFolder().getAbsolutePath() + "/metastore_db"; // Metastore 数据目录
            String warehouseUri = String.format(HIVE_WAREHOUSE_URI_FORMAT, warehouseDir);          // 格式化 Hive 仓库 URI

            // 设置 Hive 配置文件位置
            HiveConf.setHiveSiteLocation(classLoader.getResource(HiveCatalog.HIVE_SITE_FILE));
            HiveConf hiveConf = new HiveConf();

            // 设置 Hive 仓库路径
            hiveConf.setVar(
                    HiveConf.ConfVars.METASTOREWAREHOUSE,
                    TEMPORARY_FOLDER.newFolder("hive_warehouse").getAbsolutePath());

            // 设置 Hive Metastore 的连接 URI
            hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, warehouseUri);

            return hiveConf;
        } catch (IOException e) {
            // 如果临时目录创建失败或其他 IO 异常，抛出 CatalogException
            throw new CatalogException("Failed to create test HiveConf to HiveCatalog.", e);
        }
    }

    /**
     * 删除临时文件夹及其内容。
     * 这是清理测试环境的一部分，用于避免临时文件占用磁盘空间。
     */
    public static void deleteTemporaryFolder() {
        TEMPORARY_FOLDER.delete();
    }
}

