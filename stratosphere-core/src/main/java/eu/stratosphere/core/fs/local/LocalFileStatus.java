package eu.stratosphere.core.fs.local;

import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;

import java.io.File;

public class LocalFileStatus implements FileStatus {

    private final File file;

    private final Path path;

    public LocalFileStatus(File file, FileSystem fs) {
        this.file = file;
        this.path = new Path(fs.getUri().getScheme() + ":" + file.toURI().getPath());
    }

    @Override
    public long getLen() {
        return this.file.length();
    }

    @Override
    public long getBlockSize() {
        return this.file.length();
    }

    @Override
    public int getReplication() {
        return 1;
    }

    @Override
    public long getModificationTime() {
        return file.lastModified();
    }

    @Override
    public long getAccessTime() {
        return 0;
    }

    @Override
    public boolean iDir() {
        return file.isDirectory();
    }

    @Override
    public Path getPath() {
        return path;
    }
}
