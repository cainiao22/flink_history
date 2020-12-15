package eu.stratosphere.api.common.operators.util;

import java.io.Serializable;
import java.lang.annotation.Annotation;

/**
 * 用户代码的包装类接口
 *
 * @author yanpengfei
 * @date 2020/12/15
 **/
public interface UserCodeWrapper<T> extends Serializable {

  T getUserCodeObject(Class<? super T> superClass, ClassLoader cl);

  T getUserCodeObject();

  <A extends Annotation> A getUserCodeAnnotation(Class<A> annotationClass);

  Class<? extends T> getUsetCodeClass();
}
