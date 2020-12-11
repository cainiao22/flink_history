package eu.stratosphere.utils;

import eu.stratosphere.core.fs.FileSystem;

/**
 * @author ：yanpengfei
 * @date ：2020/12/10 7:09 下午
 * @description：TODO
 */
public class ClassUtils {

    private ClassUtils() {
    }

    public static Class<? extends FileSystem> getFileSystemByName(String clazzName) throws ClassNotFoundException {
        return Class.forName(clazzName, true, getClassLoader()).asSubclass(FileSystem.class);
    }

    private static ClassLoader getClassLoader() {
        return ClassUtils.getClassLoader();
    }
}
