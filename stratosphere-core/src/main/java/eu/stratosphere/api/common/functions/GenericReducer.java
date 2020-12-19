package eu.stratosphere.api.common.functions;

import eu.stratosphere.utils.Collector;

import java.util.Iterator;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/19 18:39
 */
public interface GenericReducer<T, O> extends Function {

    void reduce(Iterator<T> records, Collector<O> out) throws Exception;

    void combine(Iterator<T> records, Collector<O> out) throws Exception;
}
