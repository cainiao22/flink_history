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
public class IntCounter implements SimpleAccumulator<Integer> {

    private static final long serialVersionUID = 1L;

    private int localValue = 0;

    @Override
    public void add(Integer value) {
        localValue += value;
    }

    @Override
    public Integer getLocalValue() {
        return localValue;
    }

    @Override
    public void resetLocal() {
        localValue = 0;
    }

    @Override
    public void merge(Accumulator<Integer, Integer> other) {
        this.localValue += other.getLocalValue();
    }

    @Override
    public void read(DataInput input) throws IOException {
        localValue = input.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(localValue);
    }
}
