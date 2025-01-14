package com.hw.lineage.loader.plugin.finder;

import com.hw.lineage.loader.plugin.PluginDescriptor;
import com.hw.lineage.loader.plugin.PluginLoader;

import java.io.IOException;
import java.util.Collection;

/**
 * 此接口的实现类负责提供一种定位（发现）插件的机制，并根据发现到的插件信息创建相应的
 * {@link PluginDescriptor} 对象集合。之后，这些描述符可以用于初始化一个 {@link PluginLoader}。
 *
 * <p>换句话说，PluginFinder的作用是从指定的来源（例如文件目录、配置、网络位置等）找到所有可用的插件，
 * 并为每个插件创建一个 PluginDescriptor 对象，以便后续的插件加载器根据这些描述符来加载插件的代码与资源。
 *
 * @description: PluginFinder接口定义了查找插件的核心方法。
 * @author: HamaWhite
 */
public interface PluginFinder {

    /**
     * 查找插件并返回对应的 {@link PluginDescriptor} 实例集合。
     *
     * 实现类将根据特定的查找策略（例如在特定的文件系统目录中查找插件jar包），
     * 找到所有符合要求的插件，并为每个插件创建一个插件描述对象（PluginDescriptor）。
     *
     * @return 包含所有已发现插件对应的 PluginDescriptor 实例的集合。
     *         每个描述符包含该插件所需的所有信息（例如插件ID、插件所在的URL路径数组、额外的资源等）。
     *
     * @throws IOException 当在查找插件的过程中发生IO错误（例如目录不存在、文件访问失败）时抛出异常。
     *                     这有助于调用方区分是逻辑错误还是环境问题导致无法找到插件。
     */
    Collection<PluginDescriptor> findPlugins() throws IOException;
}
