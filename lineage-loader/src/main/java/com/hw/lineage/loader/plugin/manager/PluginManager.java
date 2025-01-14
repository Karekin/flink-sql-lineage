package com.hw.lineage.loader.plugin.manager;

import java.util.Iterator;
import java.util.Map;

/**
 * PluginManager的职责是管理集群中的插件，这些插件是通过独立的类加载器加载的。
 * 使用独立类加载器的好处是可以避免插件的依赖与本系统（Lineage）的依赖发生冲突。
 * 换句话说，PluginManager为插件提供了一种隔离加载的机制，使每个插件都可以拥有独立的依赖环境，
 * 从而降低版本冲突的风险，提高系统的模块化与可维护性。
 *
 * @description: PluginManager接口定义了与插件管理相关的方法，由实现类负责提供具体的插件加载与管理逻辑。
 *              一般在集群环境下使用，以便动态加载、卸载或查找特定类型（SPI服务接口）的插件实现。
 *
 * author: HamaWhite
 */
public interface PluginManager {

    /**
     * 根据给定的服务接口（即SPI：Service Provider Interface），为所有已知的插件返回其实现该服务的实例迭代器。
     *
     * 执行过程和意义：
     * 1. service参数是一个接口或抽象类，它是插件需要实现的服务接口（SPI）。调用此方法时，调用者希望获得
     *    系统中所有插件对该SPI的实现。
     * 2. 方法返回一个Map，键为pluginId（插件ID），值为一个Iterator<P>，用于遍历该插件提供的所有SPI实现类实例。
     * 3. 每个插件都由独立的类加载器加载，所以不同插件间的SPI实现是相互隔离的。但通过此方法，用户能一次性获取
     *    所有插件中对该SPI的实现列表，方便对比、选择或统一处理。
     *
     * 类型参数：
     * @param <P>    表示所请求的SPI服务接口的类型。例如，如果service是一个接口PluginService，那么<P>就是PluginService。
     *
     * 参数描述：
     * @param service 要查找实现类的服务接口Class对象。此参数用于反射或SPI机制来从插件中加载实现类。
     *
     * 返回值：
     * @return 一个Map，其中键为插件ID（字符串），值为一个Iterator<P>对象，用于遍历该插件提供的所有P类型的实现实例。
     *         如果某个插件不提供此SPI的实现，那么该插件不会出现在返回的Map中。
     */
    <P> Map<String, Iterator<P>> load(Class<P> service);

}
