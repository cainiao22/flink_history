package eu.stratosphere.api.common.operators.util;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.apache.commons.lang3.SerializationUtils;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2020/12/24
 **/
public class UserCodeObjectWrapper<T> implements UserCodeWrapper<T> {

    private static final long serialVersionUID = 1L;

    private T userCodeObject;

    public UserCodeObjectWrapper(T userCodeObject){
        Preconditions.checkArgument(userCodeObject instanceof Serializable, "User code object is not serializable: " + userCodeObject.getClass());
        this.userCodeObject = userCodeObject;
        Object current = userCodeObject;
        Object newCurrent = null;
        try {
            while (current != null) {
                for (Field f : current.getClass().getDeclaredFields()) {
                    f.setAccessible(true);

                    if (f.getName().contains("$outer")) {
                        newCurrent = f.get(current);

                    }

                    if (!Modifier.isStatic(f.getModifiers()) && f.get(current) != null && !(f
                        .get(current) instanceof Serializable)) {
                        throw new RuntimeException("User code object " +
                            userCodeObject + " contains non-serializable field " + f.getName()
                            + " = " + f.get(current));
                    }
                }
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Override
    public T getUserCodeObject(Class<? super T> superClass, ClassLoader cl) {
        return null;
    }

    @Override
    public T getUserCodeObject() {
        Serializable ser = (Serializable) this.userCodeObject;
        return (T) SerializationUtils.clone(ser);
    }

    @Override
    public <A extends Annotation> A getUserCodeAnnotation(Class<A> annotationClass) {
        return userCodeObject.getClass().getAnnotation(annotationClass);
    }

    @Override
    public Class<? extends T> getUsetCodeClass() {
        return (Class<? extends T>) userCodeObject.getClass();
    }
}
