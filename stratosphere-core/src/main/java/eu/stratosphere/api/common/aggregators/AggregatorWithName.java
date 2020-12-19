package eu.stratosphere.api.common.aggregators;


import eu.stratosphere.api.common.aggregators.Aggregator;
import eu.stratosphere.types.Value;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/20 00:32
 */
public class AggregatorWithName<T extends Value> {

    private final String name;

    private final Class<? extends Aggregator<T>> aggregator;


    public AggregatorWithName(String name, Class<? extends Aggregator<T>> aggregator) {
        this.name = name;
        this.aggregator = aggregator;
    }

    public String getName() {
        return name;
    }

    public Class<? extends Aggregator<T>> getAggregator() {
        return aggregator;
    }
}
