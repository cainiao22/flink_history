package eu.stratosphere.api.common.operators.base;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import eu.stratosphere.api.common.functions.GenericReducer;
import eu.stratosphere.api.common.operators.SingleInputOperator;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2020/12/24
 **/
public class ReduceOperatorBase<T extends GenericReducer> extends SingleInputOperator<T> {

    public ReduceOperatorBase(UserCodeWrapper<T> stub,
        int[] keyPositions, String name) {
        super(stub, keyPositions, name);
    }

    public ReduceOperatorBase(T stub, int[] keyPositions, String name) {
        super(new UserCodeObjectWrapper<>(stub), keyPositions, name);
    }

    public ReduceOperatorBase(Class<? extends T> udf, int[] keyPositions, String name){
        super(new UserCodeClassWrapper<>(udf), keyPositions, name);
    }

    public ReduceOperatorBase(UserCodeWrapper<T> udf, String name) {
        super(udf, name);
    }

    public ReduceOperatorBase(T udf, String name) {
        super(new UserCodeObjectWrapper<T>(udf), name);
    }

    public ReduceOperatorBase(Class<? extends T> udf, String name) {
        super(new UserCodeClassWrapper<T>(udf), name);
    }

    public boolean isCombinable(){
        return getUserCodeAnnotation(Combinable.class) != null;
    }

    @Retention(RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Combinable {}
}
