package eu.stratosphere.api.common.functions;

import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.api.common.accumulators.DoubleCounter;
import eu.stratosphere.api.common.accumulators.Histogram;
import eu.stratosphere.api.common.accumulators.IntCounter;
import eu.stratosphere.api.common.accumulators.LongCounter;
import java.util.Map;

/**
 * 运行时上下文
 *
 * @author yanpengfei
 * @date 2020/12/18
 **/
public interface RuntimeContext {

    String getTaskName();

    int getNumberOfParallelSubtasks();

    int getIndexOfSubTask();

    <V, R> void addAccumulator(String name, Accumulator<V, R> accumulator);

    <V, R> Accumulator<V, R> getAccumulator(String name);

    Map<String, Accumulator> getAllAccumulators();


    IntCounter getIntCounter(String name);

    LongCounter getLongCounter(String name);

    DoubleCounter getDoubleCounter(String name);

    Histogram getHistogram(String name);
}
