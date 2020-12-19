package eu.stratosphere.api.common.aggregators;

import eu.stratosphere.types.Value;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/20 00:35
 */
public class AggregatorRegistry {

    private Map<String, Class<? extends Aggregator>> registry;

    private Class<? extends ConvergenceCriterion> convergenceCriterion;

    private String convergenceCriterionAggregatorName;

    public void registerAggregator(String name, Class<? extends Aggregator> aggregator) {
        if (name == null || aggregator == null) {
            throw new IllegalArgumentException("Name or aggregator must not be null");
        }
        if (registry.containsKey(name)) {
            throw new RuntimeException("An aggregator is already registered under the given name.");
        }
        registry.put(name, aggregator);
    }

    public Class<? extends Aggregator> unregisterAggregator(String name) {
        return registry.remove(name);
    }

    public List<AggregatorWithName> getAllRegisteredAggregators() {
        return registry.entrySet().stream()
                .map(aggWithName -> new AggregatorWithName(aggWithName.getKey(), aggWithName.getValue()))
                .collect(Collectors.toList());
    }

    public <T extends Value> void registerAggregationConvergenceCriterion(String name, Class<? extends Aggregator<T>> aggregator,
                                                                          Class<? extends ConvergenceCriterion<T>> convergenceCheck) {
        if (name == null || aggregator == null || convergenceCheck == null) {
            throw new IllegalArgumentException("Name, aggregator, or convergence criterion must not be null");
        }
        Class<?> pre = registry.get(name);
        if (pre != null && pre != aggregator) {
            throw new RuntimeException("An aggregator is already registered under the given name.");
        }
        registry.put(name, aggregator);
        this.convergenceCriterion = convergenceCheck;
        this.convergenceCriterionAggregatorName = name;
    }

    public String getConvergenceCriterionAggregatorName() {
        return convergenceCriterionAggregatorName;
    }

    public Class<? extends ConvergenceCriterion> getConvergenceCriterion() {
        return convergenceCriterion;
    }
}
