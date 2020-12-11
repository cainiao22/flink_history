package eu.stratosphere.core.fs;

/**
 * @author ：yanpengfei
 * @date ：2020/12/11 2:55 下午
 * @description：
 */
public interface FileStatus {

    long getLen();

    long getBlockSize();

    int getReplication();

    long getModificationTime();

    long getAccessTime();

    boolean iDir();

    Path getPath();

}
