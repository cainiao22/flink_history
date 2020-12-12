package eu.stratosphere.core.fs;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author ：yanpengfei
 * @date ：2020/12/9 10:44 上午
 * @description：
 */
public abstract class FSDataInputStream extends InputStream {

    public abstract void seek(long desired) throws IOException;
}
