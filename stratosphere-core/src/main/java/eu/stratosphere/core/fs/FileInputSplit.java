package eu.stratosphere.core.fs;

import eu.stratosphere.core.io.InputSplit;
import eu.stratosphere.core.io.StringRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FileInputSplit implements InputSplit {

    private Path file;

    private long start;

    private long length;

    private String[] hosts;

    private int partitionNumber;

    /**
     * Constructs a split with host information.
     *
     * @param num    the number of this input split
     * @param file   the file name
     * @param start  the position of the first byte in the file to process
     * @param length the number of bytes in the file to process
     * @param hosts  the list of hosts containing the block, possibly <code>null</code>
     */
    public FileInputSplit(final int num, final Path file, final long start, final long length, final String[] hosts) {
        this.partitionNumber = num;
        this.file = file;
        this.start = start;
        this.length = length;
        this.hosts = hosts;
    }

    /**
     * Constructor used to reconstruct the object at the receiver of an RPC call.
     */
    public FileInputSplit() {
    }

    public Path getPath() {
        return file;
    }

    public long getStart() {
        return start;
    }

    public long getLength() {
        return length;
    }

    public String[] getHosts() {
        return hosts == null ? new String[0] : hosts;
    }

    public int getPartitionNumber() {
        return partitionNumber;
    }

    @Override
    public int getSplitNumber() {
        return partitionNumber;
    }

    /**
     * 这一对就是序列化方式
     *
     * @param input
     * @throws IOException
     */
    @Override
    public void read(DataInput input) throws IOException {
        this.partitionNumber = input.readInt();
        boolean isNull = input.readBoolean();
        if (!isNull) {
            this.file.read(input);
        }
        this.start = input.readInt();
        this.length = input.readInt();
        //读取hosts
        if (input.readBoolean()) {
            int len = input.readInt();
            this.hosts = new String[len];
            for (int i = 0; i < len; i++) {
                String host = StringRecord.readString(input);
                hosts[i] = host;
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.partitionNumber);
        if (this.file != null) {
            out.writeBoolean(true);
            this.file.write(out);
        } else {
            out.writeBoolean(false);
        }

        out.writeLong(this.start);
        out.writeLong(this.length);
        if (this.hosts != null) {
            out.writeBoolean(true);
            out.writeInt(this.hosts.length);
            for (String host : hosts) {
                StringRecord.writeString(out, host);
            }
        } else {
            out.writeBoolean(false);
        }
    }
}
