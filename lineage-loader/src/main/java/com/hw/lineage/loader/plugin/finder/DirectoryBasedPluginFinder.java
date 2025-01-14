package com.hw.lineage.loader.plugin.finder;

import com.hw.lineage.common.util.function.FunctionUtils;
import com.hw.lineage.loader.plugin.PluginDescriptor;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 基于给定的插件根目录（plugins root folder）创建一组 {@link PluginDescriptor} 的实现类。
 *
 * <p>此类使用目录结构来发现插件，每个插件对应一个单独的子目录，子目录名称即为插件ID。
 * 在该子目录中包含插件需要的所有Jar文件（包括根目录下和可能存在的lib子目录下的所有jar）。
 *
 * 目录结构期望如下：
 *
 * <pre>
 * plugins-root-folder/
 *            |------------plugin-a/ (插件a的文件夹)
 *            |                |-plugin-a-1.jar
 *            |                |-plugin-a-2.jar
 *            |                |-lib /
 *            |                     |-plugin-a-lib-1.jar
 *            |                     |-plugin-a-lib-2.jar
 *            |
 *            |------------plugin-b/ (插件b的文件夹)
 *            |                |-plugin-b-1.jar
 *            |                |-...
 *            |
 *            ... （其他插件的目录）
 * </pre>
 *
 * @description: DirectoryBasedPluginFinder 通过扫描指定的插件根目录来查找插件子目录，并为每个子目录创建相应的
 * PluginDescriptor对象，描述该插件的Jar路径信息。
 *
 * @author: HamaWhite
 */
public class DirectoryBasedPluginFinder implements PluginFinder {

    /**
     * 用于匹配目录下的jar文件的匹配模式。
     * 使用glob表达式匹配所有以".jar"结尾的文件。
     */
    private static final String JAR_MATCHER_PATTERN = "glob:**.jar";

    /**
     * 插件根目录的路径对象。
     * 在该目录下的每个子目录都会被视为一个插件目录。
     */
    private final Path pluginsRootDir;

    /**
     * 用于匹配 jar 文件的路径匹配器。
     * 基于 pluginsRootDir 的文件系统创建，用于在遍历插件目录时快速识别 jar 文件。
     */
    private final PathMatcher jarFileMatcher;

    /**
     * 构造方法
     *
     * @param pluginsRootDir 插件根目录的Path实例。该目录下的子目录将被视为插件目录。
     */
    public DirectoryBasedPluginFinder(Path pluginsRootDir) {
        this.pluginsRootDir = pluginsRootDir;
        // 使用文件系统的PathMatcher，根据JAR_MATCHER_PATTERN创建一个匹配.jar文件的匹配器
        this.jarFileMatcher = pluginsRootDir.getFileSystem().getPathMatcher(JAR_MATCHER_PATTERN);
    }

    /**
     * 从插件根目录中查找所有的插件。
     * 实现过程：
     * 1. 检查pluginsRootDir是否为一个有效的目录。
     * 2. 列出该目录下的所有子目录，每个子目录代表一个插件。
     * 3. 对每个子目录调用createPluginDescriptorForSubDirectory方法创建一个PluginDescriptor。
     * 4. 将所有PluginDescriptor收集到一个集合中返回。
     *
     * @return 包含所有已发现插件描述符的集合
     * @throws IOException 当pluginsRootDir不存在或无法访问时抛出
     */
    @Override
    public Collection<PluginDescriptor> findPlugins() throws IOException {
        // 首先检查pluginsRootDir是否存在且为目录
        if (!Files.isDirectory(pluginsRootDir)) {
            throw new IOException(
                    "Plugins root directory [" + pluginsRootDir + "] does not exist!");
        }

        // 使用Files.list列出pluginsRootDir下的所有文件和子目录，并得到一个Stream<Path>
        try (Stream<Path> stream = Files.list(pluginsRootDir)) {
            return stream
                    // 仅保留子目录，因为每个子目录代表一个插件
                    .filter(Files::isDirectory)
                    // 对每个子目录调用createPluginDescriptorForSubDirectory创建插件描述符
                    // FunctionUtils.uncheckedFunction用于包装异常，使lambda中抛出的IOException能被安全处理
                    .map(FunctionUtils.uncheckedFunction(this::createPluginDescriptorForSubDirectory))
                    // 将结果收集为一个List
                    .collect(Collectors.toList());
        }
    }

    /**
     * 为给定的插件子目录创建一个PluginDescriptor。
     * 过程：
     * 1. 调用createJarURLsFromDirectory方法递归查找该目录中的所有jar文件，并转换为URL[]。
     * 2. 对URL进行排序（按URL字符串排序）以确保稳定和可重复的加载顺序。
     * 3. 使用子目录名称作为插件ID构造PluginDescriptor对象。
     *
     * @param subDirectory 一个插件子目录的Path对象
     * @return 对应插件的PluginDescriptor对象
     * @throws IOException 当无法访问或读取该子目录中的jar文件时抛出
     */
    private PluginDescriptor createPluginDescriptorForSubDirectory(Path subDirectory)
            throws IOException {
        // 获取该子目录下所有jar文件的URL数组
        URL[] urls = createJarURLsFromDirectory(subDirectory);
        // 按URL的字符串形式排序，以确保确定性
        Arrays.sort(urls, Comparator.comparing(URL::toString));

        // 使用子目录名称作为插件ID创建PluginDescriptor
        // 这里的构造函数传入了"new String[0]"，表示此处没有额外的非jar资源
        return new PluginDescriptor(subDirectory.getFileName().toString(), urls, new String[0]);
    }

    /**
     * 从给定子目录递归寻找所有jar文件，并将这些jar文件的路径转换为URL数组返回。
     * 步骤：
     * 1. 使用Files.walk递归遍历subDirectory下的所有文件和子目录。
     * 2. 使用jarFileMatcher.matches()筛选出所有以.jar结尾的常规文件。
     * 3. 将匹配到的Path转换为URL并收集为URL数组。
     * 4. 如果在子目录中找不到任何jar文件，则抛出IOException，提示用户需要提供jar文件或删除空目录。
     *
     * @param subDirectory 插件子目录路径
     * @return 该子目录中所有jar文件对应的URL数组
     * @throws IOException 当子目录中未找到jar文件或发生IO错误时抛出
     */
    private URL[] createJarURLsFromDirectory(Path subDirectory) throws IOException {
        // 使用Files.walk递归地遍历subDirectory目录树，包括子目录和文件
        try (Stream<Path> stream = Files.walk(subDirectory)) {
            // 筛选条件：必须是普通文件（非目录）且匹配.jar模式
            URL[] urls =
                    stream.filter((Path p) -> Files.isRegularFile(p) && jarFileMatcher.matches(p))
                            // 将Path转换为URL，需要使用toUri().toURL()，并使用FunctionUtils包装处理异常
                            .map(FunctionUtils.uncheckedFunction((Path p) -> p.toUri().toURL()))
                            // 将流转换为数组
                            .toArray(URL[]::new);

            // 检查是否找到至少一个jar文件
            if (urls.length < 1) {
                throw new IOException(
                        "Cannot find any jar files for plugin in directory ["
                                + subDirectory
                                + "]."
                                + " Please provide the jar files for the plugin or delete the directory.");
            }

            return urls;
        }
    }
}
