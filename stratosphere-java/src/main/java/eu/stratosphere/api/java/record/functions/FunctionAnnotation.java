package eu.stratosphere.api.java.record.functions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/01/03 22:45
 */
public class FunctionAnnotation {

    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface ConstantFields {
        int[] value();
    }


    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface ConstantFieldsFirst {
        int[] value();
    }


    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface ConstantFieldsSecond {
        int[] value();
    }


    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface ConstantFieldsExcept {
        int[] value();
    }

    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface ConstantFieldsFirstExcept {
        int[] value();
    }

    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface ConstantFieldsSecondExcept {
        int[] value();
    }
}
