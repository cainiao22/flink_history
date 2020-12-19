package eu.stratosphere.api.common.functions;

import eu.stratosphere.utils.Collector;

/**
 * 笛卡尔积
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/19 18:46
 */
public interface GenericCrosser<V1, V2, O> extends Function {

    void cross(V1 value1, V2 value2, Collector<O> out) throws Exception;
}
