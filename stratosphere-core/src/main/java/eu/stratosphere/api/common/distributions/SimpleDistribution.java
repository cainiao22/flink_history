package eu.stratosphere.api.common.distributions;

import eu.stratosphere.types.Key;

import eu.stratosphere.utils.InstantiationUtil;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.graalvm.compiler.replacements.InstanceOfSnippetsTemplates.Instantiation;

/**
 * 简易分布式数据
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
            throw new IllegalArgumentException(
                "The bucket boundaries are of different class types.");
        }
        this.boundaries = new Key[boundaries.length][];
        Class<? extends Key> clazz = boundaries[0].getClass();
        for (int i = 0; i < boundaries.length; i++) {
            if (boundaries[i].getClass() != clazz) {
                throw new IllegalArgumentException(
                    "The bucket boundaries are of different class types.");
            }
            this.boundaries[i] = new Key[]{boundaries[i]};
        }
    }


    public SimpleDistribution(Key[][] boundaries) {
        if (boundaries == null || boundaries.length == 0) {
            throw new IllegalArgumentException(
                "The bucket boundaries are of different class types.");
        }
        this.dim = boundaries[0].length;
        Class<? extends Key>[] types = new Class[dim];
        for (int i = 0; i < this.boundaries.length; i++) {
            types[i] = boundaries[0][i].getClass();
        }
        for (int i = 0; i < boundaries.length; i++) {
            if (boundaries[i].length != dim) {
                throw new IllegalArgumentException(
                    "All bucket boundaries must have the same dimensionality.");
            }
            for (int j = 0; j < boundaries[i].length; j++) {
                if (types[j] != boundaries[i][j].getClass()) {
                    throw new IllegalArgumentException(
                        "The bucket boundaries are of different class types.");
                }
            }
        }

        this.boundaries = boundaries;
    }


    /**
     * 获取某个桶的边界
     *
     * @param bucketNum       index
     * @param totalNumBuckets 总数
     * @return
     */
    @Override
    public Key[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
        if (bucketNum < 0) {
            throw new IllegalArgumentException(
                "Requested bucket must be greater than or equal to 0.");
        } else if (bucketNum >= (totalNumBuckets - 1)) {
            throw new IllegalArgumentException(
                "Request bucket must be smaller than the total number of buckets minus 1.");
        }
        if (totalNumBuckets < 1) {
            throw new IllegalArgumentException("Total number of bucket must be larger than 0.");
        }

        int maxNumBuckets = this.boundaries.length + 1;
        if (maxNumBuckets % totalNumBuckets == 0) {
            int n = maxNumBuckets / totalNumBuckets;
            int bucketIndex = bucketNum * n + n - 1;
            return boundaries[bucketIndex];
        } else {
            throw new IllegalArgumentException(
                "Interpolation of bucket boundaries currently not supported. " +
                    "Please use an even divider of the maximum possible buckets (here: "
                    + maxNumBuckets + ") as totalBuckets.");
            // TODO: might be relaxed if much more boundary records are available than requested
        }
    }

    @Override
    public int getNumberOfFields() {
        return this.dim;
    }

    @Override
    public void read(DataInput input) throws IOException {
        this.dim = input.readInt();
        this.boundaries = new Key[input.readInt()][dim];

        Class<? extends Key>[] types = new Class[dim];
        for (int i = 0; i < dim; i++) {
            String className = input.readUTF();
            try {
                types[i] = Class.forName(className, true, getClass().getClassLoader())
                    .asSubclass(Key.class);
            } catch (ClassNotFoundException e) {
                throw new IOException("Could not load type class '" + className + "'.");
            } catch (Throwable t) {
                throw new IOException("Error loading type class '" + className + "'.", t);
            }
        }

        for (int i = 0; i < boundaries.length; i++) {
            for (int d = 0; d < dim; d++) {
                Key val = InstantiationUtil.instantiate(types[i], Key.class);
                val.read(input);
                boundaries[i][d] = val;
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(dim);
        out.writeInt(this.boundaries.length);
        for (int i = 0; i < dim; i++) {
            out.writeUTF(this.boundaries[0][i].getClass().getName());
        }
        for (Key[] boundary : this.boundaries) {
            for (int j = 0; j < dim; j++) {
                boundary[j].write(out);
            }
        }
    }
}
