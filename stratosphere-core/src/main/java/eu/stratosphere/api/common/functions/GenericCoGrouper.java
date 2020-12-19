package eu.stratosphere.api.common.functions;

import eu.stratosphere.utils.Collector;

import java.util.Iterator;

/**
 * 两组数据结合起来按key分组
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/19 18:48
 */
public interface GenericCoGrouper<V1, V2, O> extends Function {

    void coGroup(Iterator<V1> records1, Iterator<V2> records2, Collector<O> out) throws Exception;

    void combineFirst(Iterator<V1> records1, Collector<O> out) throws Exception;

    void combineSecond(Iterator<V2> records2, Collector<O> out) throws Exception;
}
