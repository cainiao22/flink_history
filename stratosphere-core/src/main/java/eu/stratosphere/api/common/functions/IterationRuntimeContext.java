package eu.stratosphere.api.common.functions;

import eu.stratosphere.api.common.aggregators.Aggregator;
import eu.stratosphere.types.Value;

public interface IterationRuntimeContext extends RuntimeContext {

    int getSuperstepNumber();

    <T extends Value> Aggregator<T> getIterationAggregator(String name);

    <T extends Value> T getPreviousIterationAggregate(String name);
}
