package eu.stratosphere.core.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * 这个类的主要功能就是对 checkpointFile 的读写操作
 */
public final class FileChannelWrapper extends FileChannel {

    private final FileSystem fs;

    private final Path checkpointFile;

    private final byte[] buf;

    private final short replication;

    private FSDataOutputStream outputStream;

    private FSDataInputStream inputStream;

    private long nextExpectedWritePosition;

    private long nextExpectedReadPosition;

    public FileChannelWrapper(FileSystem fs, Path checkpointFile, int bufferSize, short replication) {
        this.fs = fs;
        this.checkpointFile = checkpointFile;
        this.buf = new byte[bufferSize];
        this.replication = replication;
    }


    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        throw new UnsupportedOperationException("Method read is not implemented");
    }

    @Override
    public long read(ByteBuffer[] byteBuffers, int i, int i1) throws IOException {
        return 0;
    }

    @Override
    public int write(ByteBuffer byteBuffer) throws IOException {
        return 0;
    }

    @Override
    public long write(ByteBuffer[] byteBuffers, int i, int i1) throws IOException {
        throw new UnsupportedOperationException("Method write is not implemented");
    }

    @Override
    public long position() throws IOException {
        throw new UnsupportedOperationException("Method position is not implemented");
    }

    @Override
    public FileChannel position(long l) throws IOException {
        throw new UnsupportedOperationException("Method position is not implemented");
    }

    @Override
    public long size() throws IOException {
        return 0;
    }

    @Override
    public FileChannel truncate(long l) throws IOException {
        return null;
    }

    @Override
    public void force(boolean b) throws IOException {
        throw new UnsupportedOperationException("Method force is not implemented");
    }

    @Override
    public long transferTo(long l, long l1, WritableByteChannel writableByteChannel) throws IOException {
        return 0;
    }

    @Override
    public long transferFrom(ReadableByteChannel readableByteChannel, long l, long l1) throws IOException {
        return 0;
    }

    /**
     * 这里仅仅是最多读取了一个buff数量的数据，并不是全部读取的
     *
     * @param dest
     * @param position
     * @return
     * @throws IOException
     */
    @Override
    public int read(ByteBuffer dest, long position) throws IOException {
        int length = Math.min(dest.remaining(), this.buf.length);
        FSDataInputStream inputStream = getInpuStream();
        if (nextExpectedReadPosition != position) {
            System.out.println("Next expected position is " + this.nextExpectedReadPosition + ", seeking to "
                    + position);
            this.nextExpectedReadPosition = position;
            inputStream.seek(position);
        }
        int bytesRead = inputStream.read(this.buf, 0, length);
        if (bytesRead == -1) {
            return -1;
        }
        dest.put(this.buf);
        this.nextExpectedReadPosition += bytesRead;

        return bytesRead;
    }

    /**
     * 懒加载
     *
     * @return
     * @throws IOException
     */
    private FSDataInputStream getInpuStream() throws IOException {
        if (this.inputStream == null) {
            this.inputStream = this.fs.open(checkpointFile);
        }

        return this.inputStream;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        if (position != nextExpectedWritePosition) {
            throw new IOException("Next expected write position is " + this.nextExpectedWritePosition);
        }
        FSDataOutputStream outputStream = getOutputStream();
        int totalBytesWritten = 0;
        while (src.hasRemaining()) {
            int len = Math.min(src.limit(), this.buf.length);
            src.get(buf, 0, len);
            outputStream.write(buf, 0, len);
            totalBytesWritten += len;
        }
        this.nextExpectedWritePosition += totalBytesWritten;
        return totalBytesWritten;
    }

    private FSDataOutputStream getOutputStream() throws IOException {
        if (this.outputStream == null) {
            this.outputStream = this.fs.create(checkpointFile, true);
        }

        return outputStream;
    }

    @Override
    public MappedByteBuffer map(MapMode mapMode, long l, long l1) throws IOException {
        throw new UnsupportedOperationException("Method map is not implemented");
    }

    @Override
    public FileLock lock(long l, long l1, boolean b) throws IOException {
        throw new UnsupportedOperationException("Method lock is not implemented");
    }

    @Override
    public FileLock tryLock(long l, long l1, boolean b) throws IOException {
        throw new UnsupportedOperationException("Method lock is not implemented");
    }

    @Override
    protected void implCloseChannel() throws IOException {
        if (this.outputStream != null) {
            this.outputStream.close();
            this.outputStream = null;
        }
        if (this.inputStream != null) {
            this.inputStream.close();
            this.inputStream = null;
        }
    }
}
