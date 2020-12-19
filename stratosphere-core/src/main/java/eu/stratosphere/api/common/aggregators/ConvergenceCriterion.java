package eu.stratosphere.api.common.aggregators;

import eu.stratosphere.types.Value;

/**
 * 判断算法是否收敛
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/20 00:30
 */
public interface ConvergenceCriterion<T extends Value> {

    boolean isConvergenced(int iteration, T value);
}
