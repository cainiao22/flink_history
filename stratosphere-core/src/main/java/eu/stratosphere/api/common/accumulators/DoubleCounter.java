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
public class DoubleCounter implements SimpleAccumulator<Double> {

    private static final long serialVersionUID = 1L;

    private double localValue = 0;

    @Override
    public void add(Double value) {
        localValue += value;
    }

    @Override
    public Double getLocalValue() {
        return localValue;
    }

    @Override
    public void resetLocal() {
        localValue = 0;
    }

    @Override
    public void merge(Accumulator<Double, Double> other) {
        this.localValue += other.getLocalValue();
    }

    @Override
    public void read(DataInput input) throws IOException {
        localValue = input.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(localValue);
    }
}