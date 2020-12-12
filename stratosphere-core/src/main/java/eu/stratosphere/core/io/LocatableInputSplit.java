package eu.stratosphere.core.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LocatableInputSplit implements InputSplit {

    private int splitNumber;

    private String[] hostNames;


    public LocatableInputSplit(int splitNumber, String[] hostNames) {
        this.splitNumber = splitNumber;
        this.hostNames = hostNames;
    }

    /**
     * 默认方法只是做反序列化时候用的
     */
    public LocatableInputSplit() {

    }

    public String[] getHostNames() {
        return hostNames == null ? new String[0] : hostNames;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(splitNumber);
        if (this.hostNames == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(hostNames.length);
            for (String hostName : hostNames) {
                StringRecord.writeString(out, hostName);
            }
        }
    }

    @Override
    public void read(DataInput input) throws IOException {
        this.splitNumber = input.readInt();
        boolean isNull = input.readBoolean();
        if (!isNull) {
            int length = input.readInt();
            this.hostNames = new String[length];
            for (int i = 0; i < length; i++) {
                hostNames[i] = StringRecord.readString(input);
            }
        }
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }
}
