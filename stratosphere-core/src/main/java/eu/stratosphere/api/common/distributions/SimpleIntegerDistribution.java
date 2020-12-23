package eu.stratosphere.api.common.distributions;

import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Key;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 简单的Integer分布式数据集
 *
 * @author yanpengfei
 * @date 2020/12/22
 **/
public class SimpleIntegerDistribution extends SimpleDistribution {

    private static final long serialVersionUID = 1L;

    public SimpleIntegerDistribution() {
        boundaries = new IntValue[0][];
    }

    public SimpleIntegerDistribution(int[] boundaries) {
        if (boundaries == null) {
            throw new IllegalArgumentException("Bucket boundaries must not be null.");
        }
        if (boundaries.length == 0) {
            throw new IllegalArgumentException("Bucket boundaries must not be empty.");
        }
        dim = 1;
        this.boundaries = packIntegers(boundaries);
    }

    public SimpleIntegerDistribution(IntValue[] boundaries) {
        if (boundaries == null) {
            throw new IllegalArgumentException("Bucket boundaries must not be null.");
        }
        if (boundaries.length == 0) {
            throw new IllegalArgumentException("Bucket boundaries must not be empty.");
        }
        dim = 1;
        this.boundaries = new IntValue[boundaries.length][1];
        for (int i = 0; i < boundaries.length; i++) {
            this.boundaries[i] = new IntValue[]{boundaries[i]};
        }
    }

    public SimpleIntegerDistribution(IntValue[][] bucketBoundaries) {
        if (bucketBoundaries == null) {
            throw new IllegalArgumentException("Bucket boundaries must not be null.");
        }
        if (bucketBoundaries.length == 0) {
            throw new IllegalArgumentException("Bucket boundaries must not be empty.");
        }

        // dimensionality is one in this case
        dim = bucketBoundaries[0].length;

        // check the array
        for (int i = 1; i < bucketBoundaries.length; i++) {
            if (bucketBoundaries[i].length != dim) {
                throw new IllegalArgumentException(
                    "All bucket boundaries must have the same dimensionality.");
            }
        }
        this.boundaries = bucketBoundaries;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(dim);
        out.writeInt(boundaries.length);
        for (Key[] boundary : boundaries) {
            for (int j = 0; j < dim; j++) {
                out.writeInt(((IntValue) boundary[j]).getValue());
            }
        }
    }

    @Override
    public void read(DataInput input) throws IOException {
        this.dim = input.readInt();
        int len = input.readInt();
        this.boundaries = new IntValue[len][];
        for (int i = 0; i < len; i++) {
            this.boundaries[i] = new IntValue[dim];
            for (int j = 0; j < dim; j++) {
                this.boundaries[i][j] = new IntValue(input.readInt());
            }
        }
    }

    private IntValue[][] packIntegers(int[] values) {
        IntValue[][] result = new IntValue[values.length][];
        for (int i = 0; i < values.length; i++) {
            result[i] = new IntValue[]{new IntValue(values[i])};
        }
        return result;
    }
}
