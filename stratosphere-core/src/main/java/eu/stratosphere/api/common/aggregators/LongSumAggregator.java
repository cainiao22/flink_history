package eu.stratosphere.api.common.aggregators;

import eu.stratosphere.types.LongValue;

/**
 * long聚合器
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/19 19:10
 */
public class LongSumAggregator implements Aggregator<LongValue> {

    private LongValue wrapper = new LongValue();
    private long sum = 0;

    @Override
    public void aggregate(LongValue element) {
        this.sum += element.getValue();
    }

    @Override
    public LongValue getAggregate() {
        wrapper.setValue(sum);
        return wrapper;
    }

    @Override
    public void reset() {
        this.sum = 0;
    }
}
