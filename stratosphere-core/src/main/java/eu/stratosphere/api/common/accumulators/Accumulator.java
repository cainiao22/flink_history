package eu.stratosphere.api.common.accumulators;

import eu.stratosphere.core.fs.IOReadableWritable;
import java.io.Serializable;

/**
 * 累加器
 *
 * @author yanpengfei
 * @date 2020/12/18
 **/
public interface Accumulator<V, R> extends IOReadableWritable, Serializable {

    void add(V value);

    R getLocalValue();

    void resetLocal();

    void merge(Accumulator<V, R> other);

}
