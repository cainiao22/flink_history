package eu.stratosphere.api.common.distributions;

import eu.stratosphere.types.IntValue;

/**
 * 简单的Integer分布式数据集
 *
 * @author yanpengfei
 * @date 2020/12/22
 **/
public class SimpleIntegerDistribution extends SimpleDistribution {
    private static final long serialVersionUID = 1L;

    public SimpleIntegerDistribution(){
        boundaries = new IntValue[0][];
    }

    public SimpleIntegerDistribution(IntValue[] boundaries){
        if (boundaries == null)
            throw new IllegalArgumentException("Bucket boundaries must not be null.");
        if (boundaries.length == 0)
            throw new IllegalArgumentException("Bucket boundaries must not be empty.");
        dim = 1;
    }
}
