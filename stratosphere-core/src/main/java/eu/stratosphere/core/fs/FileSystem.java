package eu.stratosphere.core.fs;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author ：yanpengfei
 * @date ：2020/12/10 5:49 下午
 * @description：TODO
 */
public abstract class FileSystem {

    private static final String LOCAL_FILESYSTEM_CLASS = "eu.stratosphere.core.fs.local.LocalFileSystem";

    private static final String DISTRIBUTED_FILESYSTEM_CLASS = "eu.stratosphere.runtime.fs.hdfs.DistributedFileSystem";

    private static final String S3_FILESYSTEM_CLASS = "eu.stratosphere.runtime.fs.s3.S3FileSystem";

    private static final Object SYNCHRONIZATION_OBJECT = new Object();


    public static class FSKey {

        private String schema;

        private String authority;


        public FSKey(String schema, String authority) {
            this.schema = schema;
            this.authority = authority;
        }
    }

    public static final Map<FSKey, FileSystem> CACHE = new HashMap<FSKey, FileSystem>();
    public static final Map<String, String> FSDIRECTORY = new HashMap<String, String>();

    static {
        FSDIRECTORY.put("hdfs", DISTRIBUTED_FILESYSTEM_CLASS);
        FSDIRECTORY.put("file", LOCAL_FILESYSTEM_CLASS);
        FSDIRECTORY.put("s3", S3_FILESYSTEM_CLASS);
    }


}
