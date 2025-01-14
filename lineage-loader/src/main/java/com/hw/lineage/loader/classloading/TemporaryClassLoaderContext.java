package com.hw.lineage.loader.classloading;

/**
 * 使用 "try-with-resources" 模式设置临时上下文类加载器。
 *
 * <pre>{@code
 * try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classloader)) {
 *     // 需要使用指定上下文类加载器的代码
 * }
 * }</pre>
 *
 * <p>这种实现方式与以下代码的功能一致，但更简洁：
 *
 * <pre>{@code
 * ClassLoader original = Thread.currentThread().getContextClassLoader();
 * Thread.currentThread().setContextClassLoader(classloader);
 * try {
 *     // 需要使用指定上下文类加载器的代码
 * } finally {
 *     Thread.currentThread().setContextClassLoader(original);
 * }
 * }</pre>
 *
 * @description: TemporaryClassLoaderContext 用于临时更改线程的上下文类加载器。
 * @author: HamaWhite
 */
public class TemporaryClassLoaderContext implements AutoCloseable {

    /**
     * 设置指定的上下文类加载器，并返回一个资源对象。
     * 当资源对象被关闭时（例如在 try-with-resources 语句中），会自动将上下文类加载器恢复为原始的类加载器。
     *
     * 使用示例：
     * <pre>{@code
     * try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classloader)) {
     *     // 需要使用指定上下文类加载器的代码
     * }
     * }</pre>
     *
     * @param cl 要设置的上下文类加载器
     * @return TemporaryClassLoaderContext 对象，负责在关闭时恢复原始类加载器
     */
    public static TemporaryClassLoaderContext of(ClassLoader cl) {
        // 获取当前线程
        final Thread t = Thread.currentThread();
        // 保存当前线程的原始上下文类加载器
        final ClassLoader original = t.getContextClassLoader();

        // 将线程的上下文类加载器设置为指定的类加载器
        t.setContextClassLoader(cl);

        // 返回 TemporaryClassLoaderContext 实例
        return new TemporaryClassLoaderContext(t, original);
    }

    // 当前线程
    private final Thread thread;

    // 原始的上下文类加载器
    private final ClassLoader originalContextClassLoader;

    /**
     * 私有构造方法，用于创建 TemporaryClassLoaderContext 实例。
     *
     * @param thread 当前线程
     * @param originalContextClassLoader 原始的上下文类加载器
     */
    private TemporaryClassLoaderContext(Thread thread, ClassLoader originalContextClassLoader) {
        this.thread = thread;
        this.originalContextClassLoader = originalContextClassLoader;
    }

    /**
     * 恢复线程的上下文类加载器为原始的类加载器。
     * 此方法会在 try-with-resources 语句块结束时自动调用。
     */
    @Override
    public void close() {
        // 将线程的上下文类加载器恢复为原始的类加载器
        thread.setContextClassLoader(originalContextClassLoader);
    }
}
