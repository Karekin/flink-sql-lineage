package com.hw.lineage.loader.plugin;

import com.hw.lineage.common.util.ArrayUtils;
import com.hw.lineage.loader.classloading.ComponentClassLoader;
import com.hw.lineage.loader.classloading.TemporaryClassLoaderContext;
import com.hw.lineage.loader.plugin.manager.PluginManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * {@link PluginLoader} 是由 {@link PluginManager} 用来加载单个插件的类。
 * 它实际上是一个 {@link PluginClassLoader} 和 {@link ServiceLoader} 的结合体。
 *
 * <p>该类的核心功能是为给定的插件SPI（Service Provider Interface）在插件内部定位并加载对应的实现类。
 * 构造时需要提供一个 {@link PluginDescriptor}，该描述符包含插件所需的资源URL等信息。
 *
 * <p>加载过程大致如下：
 * 1. 使用PluginDescriptor中提供的资源URL以及排除类模式来创建一个插件专用的类加载器（PluginClassLoader）。
 * 2. 使用ServiceLoader在该类加载器下查找指定SPI的实现。
 * 3. 对外提供Iterator接口，遍历该插件对指定SPI接口的所有实现。
 *
 * @description: PluginLoader 用于加载单个插件的类及其SPI实现。
 * @author: HamaWhite
 */
public class PluginLoader implements AutoCloseable {

    /**
     * 日志记录器，用于输出插件加载相关的日志信息，便于在运行时排查问题。
     */
    private static final Logger LOG = LoggerFactory.getLogger(PluginLoader.class);

    /**
     * 插件的唯一标识符（pluginId），用于区分和识别该插件。
     */
    private final String pluginId;

    /**
     * 用于加载该插件类的类加载器。
     * 我们期望这个类加载器是线程安全的，以支持多线程同时访问插件中类的加载行为。
     */
    private final URLClassLoader pluginClassLoader;

    /**
     * 构造方法，根据给定的插件ID和类加载器创建PluginLoader实例。
     *
     * @param pluginId 插件的唯一ID，用于标识插件
     * @param pluginClassLoader 用于加载该插件中类的URLClassLoader实例
     */
    public PluginLoader(String pluginId, URLClassLoader pluginClassLoader) {
        this.pluginId = pluginId;
        this.pluginClassLoader = pluginClassLoader;
    }

    /**
     * 根据给定的插件描述符、父类加载器和始终优先父加载器的类名模式创建一个插件类加载器（PluginClassLoader）。
     *
     * <p>流程：
     * 1. 从pluginDescriptor中获取插件资源URL以及需要排除或优先父加载的类模式。
     * 2. 利用ArrayUtils.concat将始终父加载模式与插件自身排除模式合并为一组模式。
     * 3. 使用合并后的模式创建PluginClassLoader实例。
     *
     * @param pluginDescriptor 插件描述符，包含插件资源URL和排除模式
     * @param parentClassLoader 父类加载器，用于在插件类加载器无法找到类时继续委派加载
     * @param alwaysParentFirstPatterns 一组类名或前缀，这些类永远从父加载器优先加载
     * @return 为该插件创建的URLClassLoader实例（PluginClassLoader的子类）
     */
    public static URLClassLoader createPluginClassLoader(
            PluginDescriptor pluginDescriptor,
            ClassLoader parentClassLoader,
            String[] alwaysParentFirstPatterns) {
        return new PluginClassLoader(
                pluginDescriptor.getPluginResourceURLs(),
                parentClassLoader,
                ArrayUtils.concat(
                        alwaysParentFirstPatterns, pluginDescriptor.getLoaderExcludePatterns()));
    }

    /**
     * 创建并返回一个PluginLoader实例。
     * 流程：
     * 1. 根据插件描述符和给定的父类加载器与模式创建一个插件类加载器。
     * 2. 使用创建的插件类加载器与插件ID实例化PluginLoader。
     *
     * @param pluginDescriptor 插件描述符，包含插件ID、资源URL以及排除模式
     * @param parentClassLoader 父类加载器
     * @param alwaysParentFirstPatterns 强制父类加载器优先的类名模式数组
     * @return 一个新的PluginLoader实例，用于加载该插件的类和SPI实现
     */
    public static PluginLoader create(
            PluginDescriptor pluginDescriptor,
            ClassLoader parentClassLoader,
            String[] alwaysParentFirstPatterns) {
        return new PluginLoader(
                pluginDescriptor.getPluginId(),
                createPluginClassLoader(
                        pluginDescriptor, parentClassLoader, alwaysParentFirstPatterns));
    }

    /**
     * 返回该插件中对于给定服务接口（SPI）的所有实现类的迭代器。
     * 流程：
     * 1. 使用TemporaryClassLoaderContext暂时将上下文类加载器设为pluginClassLoader，确保后续ServiceLoader操作在插件类加载器下进行。
     * 2. 使用ServiceLoader.load在pluginClassLoader中查找给定service接口的实现类。
     * 3. 包装结果迭代器为ContextClassLoaderSettingIterator，在迭代时自动切换上下文类加载器。
     *
     * @param service 要查找实现的SPI接口类型
     * @param <P>     SPI接口的泛型类型
     * @return 一个Iterator<P>，可遍历该插件中所有对service接口的实现
     */
    public <P> Iterator<P> load(Class<P> service) {
        try (TemporaryClassLoaderContext ignored =
                     TemporaryClassLoaderContext.of(pluginClassLoader)) {
            return new ContextClassLoaderSettingIterator<>(
                    ServiceLoader.load(service, pluginClassLoader).iterator(), pluginClassLoader);
        }
    }
    /**
     * 关闭该PluginLoader所使用的插件类加载器。
     * <p>
     * 在插件不再需要时应调用此方法释放资源，特别是URLClassLoader可能持有文件句柄。
     * 关闭后该插件类加载器将不能再用于加载类或资源。
     */
    @Override
    public void close() {
        try {
            pluginClassLoader.close();
        } catch (IOException e) {
            // 如果在关闭类加载器时发生IO异常，记录警告日志，但不一定抛出异常中断程序
            LOG.warn("An error occurred while closing the classloader for plugin {}.", pluginId);
        }
    }

    /**
     * ContextClassLoaderSettingIterator是一个为插件SPI实现类迭代器提供上下文类加载器设置的包装器。
     * <p>
     * 当我们从插件迭代器中取出下一个SPI实现类实例时，会临时将上下文类加载器切换为插件类加载器，
     * 确保实例初始化或访问过程中需要的类加载逻辑在插件类加载环境中进行。
     * <p>
     * 避免了在遍历插件中的服务实现对象时发生类加载冲突问题。
     *
     * @param <P> 被迭代的插件服务实现类型。
     */
    static class ContextClassLoaderSettingIterator<P> implements Iterator<P> {

        /**
         * delegate是实际的SPI实现类迭代器，由ServiceLoader提供。
         * 我们对其进行包装，以在调用next()时设定合适的上下文类加载器。
         */
        private final Iterator<P> delegate;

        /**
         * pluginClassLoader是该插件对应的类加载器，在调用next()时会暂时将
         * 当前线程的上下文类加载器替换为这个插件类加载器，从而确保类加载行为一致。
         */
        private final ClassLoader pluginClassLoader;

        /**
         * 构造方法，将实际的迭代器与插件类加载器关联起来。
         *
         * @param delegate           实际的SPI实现迭代器，由ServiceLoader提供
         * @param pluginClassLoader  插件类加载器，用于在迭代过程中设定上下文类加载器
         */
        ContextClassLoaderSettingIterator(Iterator<P> delegate, ClassLoader pluginClassLoader) {
            this.delegate = delegate;
            this.pluginClassLoader = pluginClassLoader;
        }

        /**
         * 判断是否还有下一个元素，不改变上下文类加载器。
         *
         * @return 如果迭代器中仍有下一个元素则返回true，否则false
         */
        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        /**
         * 返回下一个SPI实现实例，并在此过程中暂时将上下文类加载器切换为插件类加载器。
         * 当方法结束后，上下文类加载器将恢复到原来的状态。
         *
         * @return 下一个SPI实现实例对象
         */
        @Override
        public P next() {
            // 使用TemporaryClassLoaderContext.of()暂时设置当前线程的上下文类加载器为pluginClassLoader
            try (TemporaryClassLoaderContext ignored =
                         TemporaryClassLoaderContext.of(pluginClassLoader)) {
                return delegate.next();
            }
        }
    }

    /**
     * PluginClassLoader是一个继承自ComponentClassLoader的内部静态类，用于加载插件的类和资源。
     *
     * 该类被用于：
     * 1. 从指定的URL数组（插件资源，如JAR文件）中加载类和资源。
     * 2. 将父类加载器指定为Flink的ClassLoader（或其他传入的父类加载器），以实现类的双亲委派加载机制。
     * 3. 使用allowedFlinkPackages参数控制从父类加载器中加载哪些类，其余类从插件资源中加载。
     *
     * 在此实现中，将插件的类加载完全隔离，只允许白名单中的类从父加载器加载，以避免插件与系统类路径的冲突。
     */
    private static final class PluginClassLoader extends ComponentClassLoader {

        /**
         * 构造方法，为给定插件创建一个PluginClassLoader。
         *
         * @param pluginResourceURLs    插件资源URL数组（通常是该插件的JAR文件路径）
         * @param flinkClassLoader      父类加载器（通常为Flink的类加载器或系统类加载器）
         * @param allowedFlinkPackages  允许从父类加载器加载的类前缀数组（白名单）
         */
        PluginClassLoader(
                URL[] pluginResourceURLs,
                ClassLoader flinkClassLoader,
                String[] allowedFlinkPackages) {
            // 调用父类ComponentClassLoader构造方法，传入插件URL、父类加载器、父优先包列表、组件优先包列表、以及空的已知模块映射
            super(
                    pluginResourceURLs,
                    flinkClassLoader,
                    allowedFlinkPackages,
                    new String[0],
                    Collections.emptyMap());
        }
    }
}
