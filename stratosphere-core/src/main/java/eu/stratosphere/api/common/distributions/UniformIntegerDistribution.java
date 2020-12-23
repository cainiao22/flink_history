package eu.stratosphere.api.common.distributions;

import eu.stratosphere.types.IntValue;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2020/12/23
 **/
public class UniformIntegerDistribution implements DataDistribution {

    private static final long serialVersionUID = 1L;

    private int min, max;

    public UniformIntegerDistribution(){

    }


    public UniformIntegerDistribution(int min, int max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public IntValue[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
        long diff = ((long) max) - ((long) min) + 1;
        double bucketSize = diff / ((double) totalNumBuckets);
        return new IntValue[] {new IntValue(min + (int) ((bucketNum+1) * bucketSize)) };
    }

    @Override
    public int getNumberOfFields() {
        return 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(min);
        out.writeInt(max);
    }

    @Override
    public void read(DataInput in) throws IOException {
        min = in.readInt();
        max = in.readInt();
    }
}
