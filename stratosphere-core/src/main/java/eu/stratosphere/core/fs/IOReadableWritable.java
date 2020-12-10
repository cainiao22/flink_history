package eu.stratosphere.core.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author ：yanpengfei
 * @date ：2020/12/9 10:40 上午
 * @description：TODO
 */
public interface IOReadableWritable {

    void read(DataInput input) throws IOException;

    void write(DataOutput out) throws IOException;
}
