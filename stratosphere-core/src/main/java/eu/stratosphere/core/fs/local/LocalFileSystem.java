package eu.stratosphere.core.fs.local;

import eu.stratosphere.core.fs.*;
import eu.stratosphere.utils.OperatingSystem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

public class LocalFileSystem extends FileSystem {

    private Path workingDir;

    private final URI name = OperatingSystem.isWindows() ? URI.create("file:///") : URI.create("file:/");

    /**
     * The host name of this machine;
     */
    private final String hostName;

    private static final Log LOG = LogFactory.getLog(LocalFileSystem.class);

    public LocalFileSystem() {
        this.workingDir = new Path(System.getProperty("user.dir")).makeQualified(this);
        String tmp = "unknownHost";
        try {
            tmp = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error(e);
        }
        this.hostName = tmp;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
        BlockLocation[] blockLocations = new BlockLocation[1];
        blockLocations[0] = new LocalBlockLocation(hostName, file.getLen());
        return blockLocations;
    }

    private File pathToFile(Path path) {
        if (!path.isAbsolute()) {
            path = new Path(getWorkingDirectory(), path);
        }
        //这里只要path是因为 在本地 协议是默认的
        return new File(path.toUri().getPath());
    }

    @Override
    public FileStatus getFileStatus(Path f) throws FileNotFoundException {
        File localFile = pathToFile(f);
        if (localFile.exists()) {
            return new LocalFileStatus(localFile, this);
        } else {
            throw new FileNotFoundException("File " + f + " does not exist.");
        }
    }

    @Override
    public URI getUri() {
        return name;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    protected void initialize(URI uri) {

    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        return new LocalDataInputStream(pathToFile(f));
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return open(f);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        File localFile = pathToFile(f);
        if (!localFile.exists()) {
            return null;
        }
        if (localFile.isDirectory()) {
            return new FileStatus[]{new LocalFileStatus(localFile, this)};
        }
        final String[] names = localFile.list();
        if (names == null) {
            return null;
        }
        FileStatus[] results = new FileStatus[names.length];
        for (int i = 0; i < names.length; i++) {
            results[i] = getFileStatus(new Path(f, names[i]));
        }

        return results;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        File file = pathToFile(f);
        if (file.isFile()) {
            return file.delete();
        } else if (!recursive && file.listFiles().length > 0) {
            throw new IOException("Directory " + file.toString() + " is not empty");
        }

        return delete(file);
    }

    private boolean delete(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            for (File child : children) {
                boolean isDeleted = child.delete();
                if (!isDeleted) {
                    return false;
                }
            }
        } else {
            return file.delete();
        }

        return file.delete();
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        File file = pathToFile(f);
        Path parent = f.getParent();
        return (parent == null || mkdirs(parent)) && (file.isDirectory() || file.mkdir());
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException {
        if (exists(f) && !overwrite) {
            throw new IOException("File already exists:" + f);
        }
        Path parent = f.getParent();
        if (parent != null && !mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent.toString());
        }
        File file = pathToFile(f);
        return new LocalDataOutputStream(file);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
        return create(f, overwrite, 0, (short) 0, 0);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        File srcFile = pathToFile(src);
        File dstFile = pathToFile(dst);
        return srcFile.renameTo(dstFile);
    }
}
