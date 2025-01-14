package com.hw.lineage.loader.plugin.manager;

import com.hw.lineage.loader.plugin.PluginDescriptor;
import com.hw.lineage.loader.plugin.PluginLoader;

import javax.annotation.concurrent.ThreadSafe;

import java.util.*;

/**
 * 默认的 {@link PluginManager} 实现类。
 *
 * <p>该类通过持有一组插件描述符（{@link PluginDescriptor}）对象，以及一个父类加载器（parentClassLoader），
 * 来为每个插件创建各自独立的类加载器（{@link PluginLoader}），从而实现插件的隔离加载。
 *
 * <p>此实现是线程安全的（@ThreadSafe注解表明该类的实例可安全地在多线程环境中使用），
 * 多线程同时调用其方法不会导致内部数据不一致。
 *
 * @description: DefaultPluginManager 为插件管理器的默认实现类，用于创建并管理多个插件的加载器，实现插件的独立加载与使用。
 * @author: HamaWhite
 */
@ThreadSafe
public class DefaultPluginManager implements PluginManager {

    /**
     * parentClassLoader是所有插件类加载器的父加载器。
     * 通过parentClassLoader，我们可以让插件类加载器在加载类时，
     * 首先尝试从parentClassLoader中获取特定类（尤其是一些基础类或固定需要从父级加载的类）。
     *
     * 由于parentClassLoader在这里被认为是线程安全的（例如系统类加载器或平台类加载器本身就是线程安全的），
     * 因此多线程操作时不需要对其进行额外同步。
     */
    private final ClassLoader parentClassLoader;

    /**
     * pluginDescriptors存储该PluginManager实例所管理的所有插件的描述信息。
     * 每个PluginDescriptor描述一个插件的基本信息，包括插件ID以及该插件的所有JAR文件URL。
     *
     * 通过这些描述符，可以为每个插件创建对应的PluginLoader，进而加载该插件所提供的服务实现类。
     */
    private final Collection<PluginDescriptor> pluginDescriptors;

    /**
     * alwaysParentFirstPatterns用于指定一组类名或类名模式。
     * 对于匹配这些模式的类，将始终由parentClassLoader来加载（即使用双亲委派的优先策略），
     * 而不使用插件类加载器自身的路径。
     *
     * 这样做的目的是防止某些关键类在插件中被重新定义而造成冲突，确保这些类从主程序类路径中加载。
     */
    private final String[] alwaysParentFirstPatterns;

    /**
     * 使用指定的插件描述符集合和父加载器模式初始化默认插件管理器。
     * 这里使用当前类的类加载器作为parentClassLoader。
     *
     * @param pluginDescriptors 插件描述符的集合，每个描述符描述一个可用插件
     * @param alwaysParentFirstPatterns 一组类名或模式，在加载这些类时总是先从父加载器加载
     */
    public DefaultPluginManager(
            Collection<PluginDescriptor> pluginDescriptors, String[] alwaysParentFirstPatterns) {
        this(
                pluginDescriptors,
                DefaultPluginManager.class.getClassLoader(),
                alwaysParentFirstPatterns);
    }

    /**
     * 使用指定的插件描述符集合、父类加载器和强制父加载器优先的类名模式来初始化默认插件管理器。
     *
     * @param pluginDescriptors 插件描述符集合
     * @param parentClassLoader 父类加载器，用于在插件类加载器无法找到类时继续委派加载
     * @param alwaysParentFirstPatterns 一组类的模式，这些类总是从parentClassLoader加载而不是从插件JAR加载
     */
    public DefaultPluginManager(
            Collection<PluginDescriptor> pluginDescriptors,
            ClassLoader parentClassLoader,
            String[] alwaysParentFirstPatterns) {
        this.pluginDescriptors = pluginDescriptors;
        this.parentClassLoader = parentClassLoader;
        this.alwaysParentFirstPatterns = alwaysParentFirstPatterns;
    }

    /**
     * 根据给定的服务接口（SPI），为所有已知插件创建对应的实现实例迭代器。
     *
     * <p>该方法的流程：
     * 1. 创建一个Map，键为插件ID，值为Iterator<P>，其中P是服务接口类型。
     * 2. 遍历所有插件描述符，为每个插件创建相应的PluginLoader。
     * 3. 调用PluginLoader的load方法，为给定的service接口加载该插件中对service的所有实现类，并返回一个迭代器。
     * 4. 将插件ID与相应的实现类迭代器放入map中。
     *
     * 最终返回的Map中包含了所有插件对指定SPI的实现类集合迭代器。
     *
     * @param service 要加载的服务接口类对象（通常是一个接口类型）
     * @param <P>     服务接口类型的泛型参数
     * @return 一个Map，其键为插件ID，值为该插件对该SPI服务的实现类迭代器
     */
    @Override
    public <P> Map<String, Iterator<P>> load(Class<P> service) {
        // 使用pluginDescriptors.size()初始化HashMap的容量，减少扩容，提高性能
        Map<String, Iterator<P>> pluginIteratorMap = new HashMap<>(pluginDescriptors.size());

        // 遍历所有插件描述符
        for (PluginDescriptor pluginDescriptor : pluginDescriptors) {
            // 使用该插件的描述符、parentClassLoader以及alwaysParentFirstPatterns创建对应的PluginLoader
            PluginLoader pluginLoader =
                    PluginLoader.create(
                            pluginDescriptor, parentClassLoader, alwaysParentFirstPatterns);

            // 调用pluginLoader的load方法，为给定service接口加载该插件中的所有实现
            // 将结果（一个Iterator<P>）与插件ID关联，存入pluginIteratorMap
            pluginIteratorMap.put(pluginDescriptor.getPluginId(), pluginLoader.load(service));
        }

        return pluginIteratorMap;
    }

    /**
     * 返回当前DefaultPluginManager的字符串表示，
     * 包括其parentClassLoader、pluginDescriptors和alwaysParentFirstPatterns信息，
     * 有助于调试或日志记录。
     *
     * @return 包含相关状态信息的字符串
     */
    @Override
    public String toString() {
        return "PluginManager{"
                + "parentClassLoader="
                + parentClassLoader
                + ", pluginDescriptors="
                + pluginDescriptors
                + ", alwaysParentFirstPatterns="
                + Arrays.toString(alwaysParentFirstPatterns)
                + '}';
    }
}
