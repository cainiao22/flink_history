package eu.stratosphere.api.common.operators.base;

import eu.stratosphere.api.common.functions.GenericCrosser;
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
public class CrossOperatorBase<T extends GenericCrosser> extends DualInputOperator<T> {

    public CrossOperatorBase(UserCodeWrapper<T> udf, String name) {
        super(udf, name);
    }

    public CrossOperatorBase(T udf, String name) {
        super(new UserCodeObjectWrapper<>(udf), name);
    }

    public CrossOperatorBase(Class<T> udf, String name) {
        super(new UserCodeClassWrapper<>(udf), name);
    }

    interface CrossWithSmall{}

    interface CrossWithLarge{}
}
