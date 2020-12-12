package eu.stratosphere.core.io;

import eu.stratosphere.core.fs.IOReadableWritable;

public interface InputSplit extends IOReadableWritable {

    /**
     * 获取这个分区的分区数量
     * @return
     */
    int getSplitNumber();
}
