package eu.stratosphere.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

/**
 * 反射工具类
 *
 * @author yanpengfei
 * @date 2020/12/22
 **/
public class InstantiationUtil {


    public static <T> T instantiate(Class<T> clazz, Class<? super T> castTo) {
        if (clazz == null) {
            throw new NullPointerException();
        }
        if (castTo != null && castTo.isAssignableFrom(clazz)) {
            throw new RuntimeException(
                "The class '" + clazz.getName() + "' is not a subclass of '" +
                    castTo.getName() + "' as is required.");
        }
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (InstantiationException
            | NoSuchMethodException
            | InvocationTargetException
            | IllegalAccessException e) {
            checkForInstantiation(clazz);
        }catch (Throwable t) {
            String message = t.getMessage();
            throw new RuntimeException("Could not instantiate type '" + clazz.getName() +
                "' Most likely the constructor (or a member variable initialization) threw an exception" +
                (message == null ? "." : ": " + message), t);
        }
        return null;
    }

    public static <T> void checkForInstantiation(Class<T> clazz) {
        String msg = checkForInstantiationError(clazz);
        if (msg != null) {
            throw new RuntimeException(msg);
        }
    }

    public static <T> String checkForInstantiationError(Class<T> clazz) {
        if (!isPublic(clazz)) {
            return "The class is not public.";
        }
        if (!isProperClass(clazz)) {
            return "The class is no proper class, it is either abstract, an interface, or a primitive type.";
        }
        if (isNonStaticInnerClass(clazz)) {
            return "The class is an inner class, but not statically accessible.";
        } else if (!hasPublicNullaryConstructor(clazz)) {
            return "The class has no (implicit) public nullary constructor, i.e. a constructor without arguments.";
        } else {
            return null;
        }
    }

    private static <T> boolean hasPublicNullaryConstructor(Class<T> clazz) {
        Constructor<?>[] constructors = clazz.getDeclaredConstructors();
        for (Constructor<?> constructor : constructors) {
            if (constructor.getParameterTypes().length == 0 && Modifier
                .isPublic(constructor.getModifiers())) {
                return true;
            }
        }
        return false;
    }

    public static boolean isNonStaticInnerClass(Class<?> clazz) {
        //获取该类是在那个类中定义的,如果是内部类获取的就有值
        if (clazz.getEnclosingClass() == null) {
            return false;
        }
        //如果声明了类名称 就是非匿名
        if (clazz.getDeclaringClass() != null) {
            return !Modifier.isStatic(clazz.getModifiers());
        }
        //否则就是匿名内部类
        return true;
    }

    private static <T> boolean isProperClass(Class<T> clazz) {
        return !(Modifier.isAbstract(clazz.getModifiers())
            || Modifier.isInterface(clazz.getModifiers())
            || Modifier.isNative(clazz.getModifiers()));
    }

    /**
     * 判断当前类是否公共的
     *
     * @param clazz
     * @return
     */
    public static boolean isPublic(Class<?> clazz) {
        return Modifier.isPublic(clazz.getModifiers());
    }
}
