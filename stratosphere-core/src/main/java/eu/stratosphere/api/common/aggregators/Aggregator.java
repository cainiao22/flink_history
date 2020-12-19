package eu.stratosphere.api.common.aggregators;

import eu.stratosphere.types.Value;

/**
 * 聚合器
 *
 * @author yanpengfei
 * @date 2020/12/18
 **/
public interface Aggregator<T extends Value> {

    void aggregate(T element);

    T getAggregate();

    void reset();

}
