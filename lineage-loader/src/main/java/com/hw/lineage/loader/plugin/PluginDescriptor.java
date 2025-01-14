package com.hw.lineage.loader.plugin;

import java.net.URL;
import java.util.Arrays;

/**
 * 表示一个插件的描述信息（元数据信息），用于在系统中识别和加载该插件。
 *
 * <p>PluginDescriptor主要包含三个关键信息：
 * 1. 插件的唯一标识符（pluginId）
 * 2. 插件所需加载资源的URL数组（通常是JAR文件的URL）
 * 3. 一组排除加载的类名或模式（loaderExcludePatterns），用于控制类加载行为。
 *
 * @description: PluginDescriptor为插件提供描述性元信息的类，使插件加载器（如PluginLoader）可以根据这些信息定位并加载插件的资源与类。
 * @author: HamaWhite
 */
public class PluginDescriptor {

    /**
     * 插件的唯一标识符。
     * 通常在插件加载过程中用于区分不同的插件，例如 "plugin-a"、"plugin-b"。
     */
    private final String pluginId;

    /**
     * 指向该插件相关资源（例如JAR包）的URL数组。
     * 当插件类加载器加载该插件时，会在这些URL中查找插件所需的类和资源。
     * 一般包含插件的主JAR文件，以及该插件所依赖的其他JAR文件URL。
     */
    private final URL[] pluginResourceURLs;

    /**
     * 一组类名或类名模式，用于指定那些类应始终从父加载器（而非插件类路径）加载，
     * 或者直接排除插件本身对这些类的加载。
     *
     * 与ChildFirstClassLoader中的alwaysParentFirstPatterns类似，这里提供一个机制来避免插件内部的类与系统类冲突，
     * 或者强制某些类总是从系统（父类加载器）层级加载，以确保一致性和安全性。
     */
    private final String[] loaderExcludePatterns;

    /**
     * 构造方法，用于创建一个插件描述符实例。
     *
     * @param pluginId 插件的唯一标识符
     * @param pluginResourceURLs 插件资源URL数组（指向该插件的所有相关JAR文件和资源文件）
     * @param loaderExcludePatterns 类加载排除模式数组，用于控制类的加载源
     */
    public PluginDescriptor(
            String pluginId, URL[] pluginResourceURLs, String[] loaderExcludePatterns) {
        this.pluginId = pluginId;
        this.pluginResourceURLs = pluginResourceURLs;
        this.loaderExcludePatterns = loaderExcludePatterns;
    }

    /**
     * 获取插件ID。
     *
     * @return 插件的唯一标识字符串
     */
    public String getPluginId() {
        return pluginId;
    }

    /**
     * 获取插件资源URL数组。
     *
     * @return 包含该插件所有相关资源（通常是JAR文件）的URL数组
     */
    public URL[] getPluginResourceURLs() {
        return pluginResourceURLs;
    }

    /**
     * 获取类加载排除模式数组。
     *
     * @return 用于排除或强制从父加载器加载的类名模式数组
     */
    public String[] getLoaderExcludePatterns() {
        return loaderExcludePatterns;
    }

    /**
     * 返回该PluginDescriptor的字符串表示，包括插件ID、资源URL列表和排除模式列表，便于日志与调试。
     *
     * @return 描述该插件的字符串信息
     */
    @Override
    public String toString() {
        return "PluginDescriptor{"
                + "pluginId='"
                + pluginId
                + '\''
                + ", pluginResourceURLs="
                + Arrays.toString(pluginResourceURLs)
                + ", loaderExcludePatterns="
                + Arrays.toString(loaderExcludePatterns)
                + '}';
    }
}
