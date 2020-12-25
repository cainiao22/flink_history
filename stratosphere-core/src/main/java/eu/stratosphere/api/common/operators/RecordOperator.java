package eu.stratosphere.api.common.operators;

import eu.stratosphere.types.Key;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2020/12/24
 **/
public interface RecordOperator {

    Class<? extends Key>[] getKeyClasses();
}
