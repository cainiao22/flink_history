package eu.stratosphere.api.common.operators.util;

import eu.stratosphere.utils.InstantiationUtil;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2020/12/24
 **/
public class UserCodeClassWrapper<T> implements UserCodeWrapper<T> {

    private Class<? extends T> userCodeClass;
    private String className;

    public UserCodeClassWrapper(Class<? extends T> userCodeClass) {
        this.userCodeClass = userCodeClass;
        this.className = this.userCodeClass.getName();
    }

    @Override
    public T getUserCodeObject(Class<? super T> superClass, ClassLoader cl) {
        try {
            Class<T> clazz = (Class<T>) Class.forName(className, true, cl).asSubclass(superClass);
            return InstantiationUtil.instantiate(clazz, superClass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("User code class could not be instantiated: " + e);
        }
    }

    @Override
    public T getUserCodeObject() {
        try {
            return userCodeClass.getConstructor().newInstance();
        } catch (InstantiationException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException("User code class could not be instantiated: " + e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("User code class could not be instantiated: " + e);
        }
    }

    @Override
    public <A extends Annotation> A getUserCodeAnnotation(Class<A> annotationClass) {
        return userCodeClass.getAnnotation(annotationClass);
    }

    @Override
    public Class<? extends T> getUsetCodeClass() {
        return this.userCodeClass;
    }
}
