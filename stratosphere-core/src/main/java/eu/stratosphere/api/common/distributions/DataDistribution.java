package eu.stratosphere.api.common.distributions;

import eu.stratosphere.core.fs.IOReadableWritable;
import eu.stratosphere.types.Key;

import java.io.Serializable;

/**
 * 分布式数据集
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/20 01:53
 */
public interface DataDistribution extends IOReadableWritable, Serializable {

    //获取数据边界
    Key[] getBucketBoundary(int bucketNum, int totalNumBuckets);

    int getNumberOfFields();
}
