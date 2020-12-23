package eu.stratosphere.api.common.distributions;

import eu.stratosphere.types.DoubleValue;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2020/12/23
 **/
public class UniformDoubleDistribution implements DataDistribution {

    private static final long serialVersionUID = 1L;

    private double min, max;


    public UniformDoubleDistribution() {
    }

    public UniformDoubleDistribution(double min, double max) {
        this.min = min;
        this.max = max;
    }


    @Override
    public DoubleValue[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
        double bucketSize = max - min + 1;
        return new DoubleValue[]{new DoubleValue(min + (bucketNum + 1) * bucketSize)};
    }

    @Override
    public int getNumberOfFields() {
        return 1;
    }

    @Override
    public void read(DataInput input) throws IOException {
        this.min = input.readDouble();
        this.max = input.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.min);
        out.writeDouble(this.max);
    }
}
