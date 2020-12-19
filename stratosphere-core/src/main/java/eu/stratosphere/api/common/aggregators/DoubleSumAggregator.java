package eu.stratosphere.api.common.aggregators;

import eu.stratosphere.types.DoubleValue;

/**
 * double聚合器
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/19 18:54
 */
public class DoubleSumAggregator implements Aggregator<DoubleValue> {

    private DoubleValue wrappter = new DoubleValue();

    private double sum;

    @Override
    public DoubleValue getAggregate() {
        wrappter.setValue(sum);
        return wrappter;
    }

    @Override
    public void aggregate(DoubleValue element) {
        this.sum += element.getValue();
    }

    @Override
    public void reset() {
        this.sum = 0;
    }
}
