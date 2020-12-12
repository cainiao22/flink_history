package eu.stratosphere.core.fs.local;

import eu.stratosphere.core.fs.FSDataOutputStream;

import java.io.*;

public class LocalDataOutputStream extends FSDataOutputStream {

    private FileOutputStream fos;

    public LocalDataOutputStream(File file) throws IOException {
        this.fos = new FileOutputStream(file);
    }

    @Override
    public void write(int i) throws IOException {
        fos.write(i);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        fos.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        fos.close();
    }
}
