package eu.stratosphere.api.common.functions;

import eu.stratosphere.utils.Collector;

/**
 * join操作
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/19 18:43
 */
public interface GenericJoiner<V1, V2, O> extends Function {

    void join(V1 value1, V2 value2, Collector<O> out) throws Exception;
}
