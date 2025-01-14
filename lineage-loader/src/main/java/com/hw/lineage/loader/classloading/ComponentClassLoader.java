package com.hw.lineage.loader.classloading;

import com.google.common.collect.Iterators;
import com.hw.lineage.common.util.function.FunctionWithException;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * 一个继承自 {@link URLClassLoader} 的类加载器，用于在给定的类路径中加载类，并根据特定的包名策略
 * 限制可加载类的范围。本类可根据配置实现三种类加载顺序：
 *
 * 1. component-only: 仅从component（本类）和引导类加载器（bootstrap）加载类（默认）
 * 2. component-first: 优先从component加载，如果未找到，再从引导类加载器和owner（外部拥有者类加载器）中加载
 * 3. owner-first: 优先从owner加载，如果未找到，再从component和引导类加载器中加载
 *
 * 类加载器层次结构描述如下（文本中所述的owner、component和bootstrap）：
 * <pre>
 *       Owner     Bootstrap
 *           ^         ^
 *           |---------|
 *                |
 *            Component
 * </pre>
 *
 * 从上图可看出，Owner、Bootstrap、Component之间的调用层次关系。
 *
 * 本类可根据指定的包前缀决定类和资源的加载顺序，从而实现模块级别的隔离和优先级控制。
 *
 * @description: ComponentClassLoader
 * @author: HamaWhite
 */
public class ComponentClassLoader extends URLClassLoader {

    // 静态初始化的平台或引导类加载器的引用
    private static final ClassLoader PLATFORM_OR_BOOTSTRAP_LOADER;

    // 用于指向"拥有者"的类加载器（ownerClassLoader），即可能是上层的类加载器
    private final ClassLoader ownerClassLoader;

    // 指定需要以owner优先加载的包前缀列表
    private final String[] ownerFirstPackages;
    // 指定需要以component优先加载的包前缀列表
    private final String[] componentFirstPackages;

    // 对应资源加载顺序中，owner优先的资源前缀（通过将包名转换成路径前缀获得）
    private final String[] ownerFirstResourcePrefixes;
    // 对应资源加载顺序中，component优先的资源前缀（同上）
    private final String[] componentFirstResourcePrefixes;

    // 已知的包前缀与模块名称的映射关系，用于在找不到类时给出更清晰的错误提示。
    // 当尝试加载某个类失败时，如果该类的包前缀在此映射中，则提示用户可能需要引入对应的模块。
    private final Map<String, String> knownPackagePrefixesModuleAssociation;

    /**
     * 构造方法
     *
     * @param classpath 类路径URL数组，用于指定component层需要加载类和资源的来源
     * @param ownerClassLoader 指定owner类加载器，即上层类加载器
     * @param ownerFirstPackages 需要优先从owner加载的包前缀列表
     * @param componentFirstPackages 需要优先从component加载的包前缀列表
     * @param knownPackagePrefixesModuleAssociation 已知包前缀到模块名的映射，用于异常提示
     */
    public ComponentClassLoader(
            URL[] classpath,
            ClassLoader ownerClassLoader,
            String[] ownerFirstPackages,
            String[] componentFirstPackages,
            Map<String, String> knownPackagePrefixesModuleAssociation) {
        // 调用父类URLClassLoader构造方法，并以PLATFORM_OR_BOOTSTRAP_LOADER为父加载器
        super(classpath, PLATFORM_OR_BOOTSTRAP_LOADER);
        this.ownerClassLoader = ownerClassLoader;

        this.ownerFirstPackages = ownerFirstPackages;
        this.componentFirstPackages = componentFirstPackages;

        this.knownPackagePrefixesModuleAssociation = knownPackagePrefixesModuleAssociation;

        // 将ownerFirstPackages和componentFirstPackages的包前缀转换成资源路径前缀（将'.'替换为'/'）
        ownerFirstResourcePrefixes = convertPackagePrefixesToPathPrefixes(ownerFirstPackages);
        componentFirstResourcePrefixes = convertPackagePrefixesToPathPrefixes(componentFirstPackages);
    }

    // ----------------------------------------------------------------------------------------------
    // Class loading
    // ----------------------------------------------------------------------------------------------

    /**
     * 覆写类加载方法。当尝试加载一个类时，将根据配置的优先级策略（component-only、component-first、owner-first）
     * 决定从哪个类加载器先尝试加载该类。
     *
     * @param name    要加载的类的全限定名（包名+类名）
     * @param resolve 指定是否在加载后立即解析类的引用和依赖（如果为true，则会调用resolveClass方法）
     * @return 已加载的Class对象
     * @throws ClassNotFoundException 如果无法在component（本类）或owner中找到该类，则抛出此异常
     */
    @Override
    protected Class<?> loadClass(final String name, final boolean resolve)
            throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            // 使用类名获取加载锁，以防止多线程同时加载相同类时的竞态条件
            try {
                // 首先检查该类是否已经被加载过
                final Class<?> loadedClass = findLoadedClass(name);
                if (loadedClass != null) {
                    // 如果已加载过，则根据resolve参数决定是否解析并直接返回
                    return resolveIfNeeded(resolve, loadedClass);
                }

                // 根据类名决定加载策略
                if (isComponentFirstClass(name)) {
                    // 如果该类属于component优先的包前缀，则采用component-first策略
                    return loadClassFromComponentFirst(name, resolve);
                }
                if (isOwnerFirstClass(name)) {
                    // 如果该类属于owner优先的包前缀，则采用owner-first策略
                    return loadClassFromOwnerFirst(name, resolve);
                }

                // 如果不属于上述任何优先级表，则默认component-only策略
                return loadClassFromComponentOnly(name, resolve);
            } catch (ClassNotFoundException e) {
                // 如果在此过程中未能找到类，那么检查knownPackagePrefixesModuleAssociation映射
                // 如果类的包前缀在映射中存在，则提示用户可能需要添加相应模块到类路径
                Optional<String> foundAssociatedModule =
                        knownPackagePrefixesModuleAssociation.entrySet().stream()
                                .filter(entry -> name.startsWith(entry.getKey()))
                                .map(Map.Entry::getValue)
                                .findFirst();
                if (foundAssociatedModule.isPresent()) {
                    throw new ClassNotFoundException(
                            String.format(
                                    "Class '%s' not found. Perhaps you forgot to add the module '%s' to the classpath?",
                                    name, foundAssociatedModule.get()),
                            e);
                }
                // 如果不存在任何匹配的包前缀，则直接抛出ClassNotFoundException
                throw e;
            }
        }
    }

    /**
     * 如果需要解析类（resolve参数为true），则调用resolveClass进行解析。
     * @param resolve 是否解析类
     * @param loadedClass 已加载的类对象
     * @return 返回已加载（并可能已解析）的类对象
     */
    private Class<?> resolveIfNeeded(final boolean resolve, final Class<?> loadedClass) {
        if (resolve) {
            resolveClass(loadedClass);
        }
        return loadedClass;
    }

    /**
     * 判断给定类名是否应该owner优先加载（即类名是否以ownerFirstPackages中的任意前缀开始）
     */
    private boolean isOwnerFirstClass(final String name) {
        return Arrays.stream(ownerFirstPackages).anyMatch(name::startsWith);
    }

    /**
     * 判断给定类名是否应该component优先加载（即类名是否以componentFirstPackages中的任意前缀开始）
     */
    private boolean isComponentFirstClass(final String name) {
        return Arrays.stream(componentFirstPackages).anyMatch(name::startsWith);
    }

    /**
     * 使用component-only策略加载类，即直接调用父类URLClassLoader的loadClass方法进行加载。
     * @param name 类名
     * @param resolve 是否解析
     */
    private Class<?> loadClassFromComponentOnly(final String name, final boolean resolve)
            throws ClassNotFoundException {
        return super.loadClass(name, resolve);
    }

    /**
     * 使用component-first策略加载类：
     * 1. 首先尝试从component加载（this类加载器）。
     * 2. 如果失败（ClassNotFoundException或NoClassDefFoundError），再尝试从owner加载。
     */
    private Class<?> loadClassFromComponentFirst(final String name, final boolean resolve)
            throws ClassNotFoundException {
        try {
            return loadClassFromComponentOnly(name, resolve);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            return loadClassFromOwnerOnly(name, resolve);
        }
    }

    /**
     * 使用owner-only策略加载类，即只从ownerClassLoader加载该类。
     * @param name 类名
     * @param resolve 是否解析
     */
    private Class<?> loadClassFromOwnerOnly(final String name, final boolean resolve)
            throws ClassNotFoundException {
        return resolveIfNeeded(resolve, ownerClassLoader.loadClass(name));
    }

    /**
     * 使用owner-first策略加载类：
     * 1. 首先尝试从ownerClassLoader加载。
     * 2. 如果失败，则从component加载。
     */
    private Class<?> loadClassFromOwnerFirst(final String name, final boolean resolve)
            throws ClassNotFoundException {
        try {
            return loadClassFromOwnerOnly(name, resolve);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            return loadClassFromComponentOnly(name, resolve);
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Resource loading
    // ----------------------------------------------------------------------------------------------

    /**
     * 重写getResource方法。
     * 当请求加载某个资源（如类文件、配置文件）时，先尝试使用getResources获取此资源对应的Enumeration<URL>，
     * 如果有至少一个URL，则返回第一个URL，否则返回null。
     *
     * @param name 资源名称（通常是相对于类路径的路径，如"com/example/MyClass.class"）
     * @return 资源的URL，如果未找到则返回null
     */
    @Override
    public URL getResource(final String name) {
        try {
            final Enumeration<URL> resources = getResources(name);
            if (resources.hasMoreElements()) {
                return resources.nextElement();
            }
        } catch (IOException ignored) {
            // 如果加载过程中出现IO异常，这里忽略并返回null，模仿JDK默认行为
        }
        return null;
    }

    /**
     * 重写getResources方法。
     * 根据资源名判断应采用的资源加载策略（component-first、owner-first或component-only）来获取资源的枚举。
     *
     * @param name 资源名
     * @return 包含所有匹配资源URL的Enumeration
     * @throws IOException 当IO错误发生时抛出
     */
    @Override
    public Enumeration<URL> getResources(final String name) throws IOException {
        // 根据资源路径前缀判断加载策略
        if (isComponentFirstResource(name)) {
            return loadResourceFromComponentFirst(name);
        }
        if (isOwnerFirstResource(name)) {
            return loadResourceFromOwnerFirst(name);
        }

        // 默认策略：component-only
        return loadResourceFromComponentOnly(name);
    }

    /**
     * 判断该资源是否应以owner-first方式加载，即资源名称是否以ownerFirstResourcePrefixes中的任意前缀开始
     *
     * @param name 资源名称
     */
    private boolean isOwnerFirstResource(final String name) {
        return Arrays.stream(ownerFirstResourcePrefixes).anyMatch(name::startsWith);
    }

    /**
     * 判断该资源是否应以component-first方式加载，即资源名称是否以componentFirstResourcePrefixes中的任意前缀开始
     *
     * @param name 资源名称
     */
    private boolean isComponentFirstResource(final String name) {
        return Arrays.stream(componentFirstResourcePrefixes).anyMatch(name::startsWith);
    }

    /**
     * 使用component-only策略获取资源，即仅从本类加载器的类路径中查找资源。
     *
     * @param name 资源名
     * @return 返回由本类加载器查找到的资源URL枚举
     * @throws IOException 如果查找过程中出现IO异常
     */
    private Enumeration<URL> loadResourceFromComponentOnly(final String name) throws IOException {
        return super.getResources(name);
    }

    /**
     * 使用component-first策略获取资源：
     * 1. 首先尝试从component（本类加载器）获取资源。
     * 2. 如果有需要，可再从owner加载器获取并合并结果。
     *
     * @param name 资源名
     * @return 合并component和owner查找结果的URL枚举
     * @throws IOException 如果查找过程中出现IO异常
     */
    private Enumeration<URL> loadResourceFromComponentFirst(final String name) throws IOException {
        return loadResourcesInOrder(
                name,
                this::loadResourceFromComponentOnly,
                this::loadResourceFromOwnerOnly);
    }

    /**
     * 使用owner-only策略获取资源，即仅从ownerClassLoader查找资源。
     *
     * @param name 资源名
     * @return 由ownerClassLoader查找到的资源URL枚举
     * @throws IOException 如果查找过程中出现IO异常
     */
    private Enumeration<URL> loadResourceFromOwnerOnly(final String name) throws IOException {
        return ownerClassLoader.getResources(name);
    }

    /**
     * 使用owner-first策略获取资源：
     * 1. 首先尝试从owner加载资源。
     * 2. 如果存在，再从component加载资源，并将结果合并为一个Enumeration。
     *
     * @param name 资源名
     * @return 合并owner和component查找结果的URL枚举
     * @throws IOException 如果查找过程中出现IO异常
     */
    private Enumeration<URL> loadResourceFromOwnerFirst(final String name) throws IOException {
        return loadResourcesInOrder(
                name,
                this::loadResourceFromOwnerOnly,
                this::loadResourceFromComponentOnly);
    }

    /**
     * ResourceLoadingFunction定义一个函数式接口，用于根据资源名来获取Enumeration<URL>。
     * 可抛出IOException。
     */
    private interface ResourceLoadingFunction
            extends FunctionWithException<String, Enumeration<URL>, IOException> {
    }

    /**
     * 将两个资源加载函数的结果以指定顺序合并。
     * 首先从firstClassLoader中查找资源，再从secondClassLoader中查找，将两者结果合并为单个Enumeration。
     *
     * @param name 资源名
     * @param firstClassLoader 第一个查找资源的函数
     * @param secondClassLoader 第二个查找资源的函数
     * @return 合并后的URL枚举
     * @throws IOException 如果查找过程发生IO错误
     */
    private Enumeration<URL> loadResourcesInOrder(
            String name,
            ResourceLoadingFunction firstClassLoader,
            ResourceLoadingFunction secondClassLoader)
            throws IOException {
        final Iterator<URL> iterator =
                // 使用Guava的Iterators将两个Enumeration转换为Iterator并合并
                Iterators.concat(
                        Iterators.forEnumeration(firstClassLoader.apply(name)),
                        Iterators.forEnumeration(secondClassLoader.apply(name)));

        return new IteratorBackedEnumeration<>(iterator);
    }

    /**
     * IteratorBackedEnumeration是一个适配器，将Iterator转换为Enumeration。
     *
     * @param <T> 元素类型
     */
    static class IteratorBackedEnumeration<T> implements Enumeration<T> {

        private final Iterator<T> backingIterator;

        public IteratorBackedEnumeration(Iterator<T> backingIterator) {
            this.backingIterator = backingIterator;
        }

        @Override
        public boolean hasMoreElements() {
            return backingIterator.hasNext();
        }

        @Override
        public T nextElement() {
            return backingIterator.next();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Utils
    // ----------------------------------------------------------------------------------------------

    /**
     * 将包前缀数组转换为资源路径前缀数组。
     * 例如：将"com.example"转换为"com/example"。
     * 这样在加载资源时，可以根据资源路径前缀来判定资源的加载策略。
     *
     * @param packagePrefixes 包前缀数组，例如：["com.example", "org.apache"]
     * @return 对应的资源路径前缀数组，例如：["com/example", "org/apache"]
     */
    private static String[] convertPackagePrefixesToPathPrefixes(String[] packagePrefixes) {
        return Arrays.stream(packagePrefixes)
                .map(packageName -> packageName.replace('.', '/'))
                .toArray(String[]::new);
    }

    // 静态代码块，在类加载时执行，用于初始化PLATFORM_OR_BOOTSTRAP_LOADER
    static {
        ClassLoader platformLoader = null;
        try {
            // 尝试在Java 9+环境中通过反射获取平台类加载器（Platform Class Loader）
            // Java 9后引入了Platform Class Loader，它与Bootstrap Class Loader共同构成基础类加载器层次。
            platformLoader =
                    (ClassLoader) ClassLoader.class.getMethod("getPlatformClassLoader").invoke(null);
        } catch (NoSuchMethodException e) {
            // 在Java 8中，不存在getPlatformClassLoader方法，此时使用null表示Bootstrap Class Loader
            // 这样当传递给URLClassLoader的父类加载器为null时，会表示使用引导类加载器。
        } catch (Exception e) {
            // 如果在Java 9+中获取平台类加载器时出现其他异常，则抛出IllegalStateException
            // 表示无法正确获取平台类加载器
            throw new IllegalStateException("Cannot retrieve platform classloader on Java 9+", e);
        }
        // 初始化静态常量PLATFORM_OR_BOOTSTRAP_LOADER
        // 在Java 9+上为platformLoader，在Java 8上为null（表示Bootstrap Class Loader）
        PLATFORM_OR_BOOTSTRAP_LOADER = platformLoader;

        // 注册为并行可用的类加载器，以便多线程加载类时提高并发性能
        ClassLoader.registerAsParallelCapable();
    }
}



