package eu.stratosphere.core.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 仅仅有一个分区号
 */
public class GenericInputSplit implements InputSplit {

    protected int number;

    /**
     * Default constructor for instantiation during de-serialization.
     */
    public GenericInputSplit() {}

    /**
     * Creates a generic input split with the given split number.
     *
     * @param number
     *        the number of the split
     */
    public GenericInputSplit(final int number) {
        this.number = number;
    }

    @Override
    public int getSplitNumber() {
        return number;
    }

    @Override
    public void read(DataInput input) throws IOException {
        this.number = input.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(number);
    }

    public String toString() {
        return "[" + this.number + "]";
    }
}
