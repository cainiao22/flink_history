package eu.stratosphere.api.common.distributions;

import eu.stratosphere.types.Key;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/20 02:05
 */
public class SimpleDistribution implements DataDistribution {

    private static final long serialVersionUID = 1L;

    protected Key[][] boundaries;

    protected int dim;

    public SimpleDistribution() {
        this.boundaries = new Key[0][];
    }

    public SimpleDistribution(Key[] boundaries) {
        this.dim = 1;
        if (boundaries == null || boundaries.length == 0) {
            throw new IllegalArgumentException("The bucket boundaries are of different class types.");
        }
        this.boundaries = new Key[boundaries.length][];
        Class<? extends Key> clazz = boundaries[0].getClass();
        for (int i = 0; i < boundaries.length; i++) {
            if (boundaries[i].getClass() != clazz) {
                throw new IllegalArgumentException("The bucket boundaries are of different class types.");
            }
            this.boundaries[i] = new Key[]{boundaries[i]};
        }
    }


    public SimpleDistribution(Key[][] boundaries) {
        if (boundaries == null || boundaries.length == 0) {
            throw new IllegalArgumentException("The bucket boundaries are of different class types.");
        }
        this.dim = boundaries[0].length;
        Class<? extends Key>[] types = new Class[dim];
        for (int i = 0; i < this.boundaries.length; i++) {
            types[i] = boundaries[0][i].getClass();
        }
        for (int i = 0; i < boundaries.length; i++) {
            if (boundaries[i].length != dim) {
                throw new IllegalArgumentException("All bucket boundaries must have the same dimensionality.");
            }
            for (int j = 0; j < boundaries[i].length; j++) {
                if (types[j] != boundaries[i][j].getClass()) {
                    throw new IllegalArgumentException("The bucket boundaries are of different class types.");
                }
            }
        }

        this.boundaries = boundaries;
    }


    @Override
    public Key[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
        return new Key[0];
    }

    @Override
    public int getNumberOfFields() {
        return this.dim;
    }

    @Override
    public void read(DataInput input) throws IOException {

    }

    @Override
    public void write(DataOutput out) throws IOException {

    }
}
