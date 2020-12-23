package eu.stratosphere.api.common.operators;

import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2020/12/23
 **/
public abstract class AbstractUdfOperator<T extends Function> extends Operator {

    protected final UserCodeWrapper<T> stub;

    protected AbstractUdfOperator(UserCodeWrapper<T> stub, String name) {
        super(name);
        this.stub = stub;
    }

    @Override
    public UserCodeWrapper<?> getUserCodeWrapper() {
        return stub;
    }

    public abstract int getNumberOfInputs();

    public abstract int[] getKeyColumns(int inputNum);

    protected static <U> Class<U>[] asArray(Class<U> clazz) {
        Class<U>[] array = new Class[]{clazz};
        return array;
    }

    protected static <U> Class<U>[] emptyClassArray() {
        Class<U>[] array = (Class<U>[]) new Class[0];
        return array;
    }
}
