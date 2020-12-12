package eu.stratosphere.core.fs.local;

import eu.stratosphere.core.fs.FSDataInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class LocalDataInputStream extends FSDataInputStream {

    private FileInputStream fis;

    public LocalDataInputStream(File file) throws IOException {
        this.fis = new FileInputStream(file);
    }

    @Override
    public void seek(long desired) throws IOException {
        fis.getChannel().position(desired);
    }

    @Override
    public int read() throws IOException {
        return this.fis.read();
    }

    @Override
    public void close() throws IOException {
        this.fis.close();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return this.fis.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return this.fis.skip(n);
    }
}
