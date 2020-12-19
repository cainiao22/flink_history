package eu.stratosphere.api.common.accumulators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2020/12/18
 **/
public class LongCounter implements SimpleAccumulator<Long> {

    private static final long serialVersionUID = 1L;

    private long localValue = 0;

    @Override
    public void add(Long value) {
        localValue += value;
    }

    @Override
    public Long getLocalValue() {
        return localValue;
    }

    @Override
    public void resetLocal() {
        localValue = 0;
    }

    @Override
    public void merge(Accumulator<Long, Long> other) {
        this.localValue += other.getLocalValue();
    }

    @Override
    public void read(DataInput input) throws IOException {
        localValue = input.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(localValue);
    }
}
