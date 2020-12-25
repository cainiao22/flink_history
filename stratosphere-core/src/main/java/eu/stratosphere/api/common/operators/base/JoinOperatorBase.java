package eu.stratosphere.api.common.operators.base;

import eu.stratosphere.api.common.functions.GenericJoiner;
import eu.stratosphere.api.common.operators.DualInputOperator;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2020/12/24
 **/
public class JoinOperatorBase<T extends GenericJoiner<?, ?, ?>> extends DualInputOperator<T> {

    public JoinOperatorBase(UserCodeWrapper<T> udf, int[] keyPositions1, int[] keyPositions2, String name){
        super(udf, keyPositions1, keyPositions2, name);
    }

    public JoinOperatorBase(T udf, int[] keyPositions1, int[] keyPositions2, String name) {
        super(new UserCodeObjectWrapper<T>(udf), keyPositions1, keyPositions2, name);
    }

    public JoinOperatorBase(Class<? extends T> udf, int[] keyPositions1, int[] keyPositions2, String name) {
        super(new UserCodeClassWrapper<T>(udf), keyPositions1, keyPositions2, name);
    }
}
