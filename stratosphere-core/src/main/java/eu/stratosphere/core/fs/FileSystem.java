package eu.stratosphere.core.fs;

import eu.stratosphere.utils.ClassUtils;
import eu.stratosphere.utils.OperatingSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

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

    public static final Map<FSKey, FileSystem> CACHE = new HashMap<>();
    public static final Map<String, String> FSDIRECTORY = new HashMap<String, String>();

    static {
        FSDIRECTORY.put("hdfs", DISTRIBUTED_FILESYSTEM_CLASS);
        FSDIRECTORY.put("file", LOCAL_FILESYSTEM_CLASS);
        FSDIRECTORY.put("s3", S3_FILESYSTEM_CLASS);
    }

    public static FileSystem getLocalFileSystem() throws IOException {
        URI uri = null;
        try {
            uri = OperatingSystem.isWindows() ? new URI("file:/") : new URI("file:///");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        return get(uri);
    }

    public static FileSystem get(URI uri) throws IOException {

        if (uri == null) {
            throw new IOException("No file system found with scheme " + uri.getScheme());
        }
        FileSystem fs = null;
        synchronized (SYNCHRONIZATION_OBJECT) {
            FSKey fsKey = new FSKey(uri.getScheme(), uri.getAuthority());
            if (CACHE.containsKey(fsKey)) {
                return CACHE.get(fsKey);
            }
            // Try to create a new file system
            if (!FSDIRECTORY.containsKey(uri.getScheme())) {
                throw new IOException("No file system found with scheme " + uri.getScheme());
            }

            Class<? extends FileSystem> clazz = null;

            try {
                clazz = ClassUtils.getFileSystemByName(FSDIRECTORY.get(uri.getScheme()));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e.getMessage());
            }

            try {
                fs = clazz.newInstance();
                fs.initialize(uri);
                CACHE.put(fsKey, fs);
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return fs;
    }

    protected abstract void initialize(URI uri);


    public abstract Path getWorkingDirectory();

    public abstract URI getUri();

    public abstract FileStatus getFileStatus(Path f) throws FileNotFoundException;

    public abstract BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException;

    public abstract FSDataInputStream open(Path f, int bufferSize) throws IOException;

    public abstract FSDataInputStream open(Path f) throws IOException;

    public long getDefaultBlockSize() {
        return 32 * 1024 * 1024; //32Mb
    }

    public abstract FileStatus[] listStatus(Path f) throws IOException;

    public boolean exists(Path f) throws IOException {
        try {
            return getFileStatus(f) != null;
        } catch (FileNotFoundException e) {
            return false;
        }
    }


    public abstract boolean mkdirs(Path f) throws IOException;

    public abstract FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication,
                                              long blockSize) throws IOException;

    public abstract FSDataOutputStream create(Path f, boolean overwrite) throws IOException;

    public abstract boolean rename(Path src, Path dst) throws IOException;

    public abstract boolean delete(Path f, boolean recursive) throws IOException;

    public long getNumberOfBlocks(final FileStatus file) throws IOException {
        int numberOfBlocks = 0;
        if (file == null) {
            return 0;
        }
        if (!file.isDir()) {
            return getNumberOfBlocks(file.getLen(), file.getBlockSize());
        }

        FileStatus[] files = this.listStatus(file.getPath());
        for (FileStatus f : files) {
            numberOfBlocks += getNumberOfBlocks(f.getLen(), f.getBlockSize());
        }

        return numberOfBlocks;
    }

    private long getNumberOfBlocks(final long length, final long blocksize) {
        if (blocksize != 0) {
            return (length + blocksize - 1) / blocksize;
        }

        return 1;
    }
}
