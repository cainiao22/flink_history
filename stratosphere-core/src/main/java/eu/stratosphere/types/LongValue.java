package eu.stratosphere.types;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Long类型的包装类
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/19 19:10
 */
public class LongValue implements NormalizableKey, CopyableValue<LongValue> {

    private long value;

    public LongValue() {
        this.value = 0;
    }

    public LongValue(long value) {
        this.value = value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }


    @Override
    public int getBinaryLength() {
        return 8;
    }

    @Override
    public void copyTo(LongValue target) {
        target.setValue(this.value);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeLong(source.readLong());
    }

    @Override
    public void read(DataInput input) throws IOException {
        this.value = input.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.value);
    }

    @Override
    public int compareTo(Key o) {
        if (!(o instanceof LongValue)) {
            throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to N_Integer!");
        }

        final long other = ((LongValue) o).value;

        return Long.compare(this.value, other);
    }

    @Override
    public int getMaxNormalizedKeyLen() {
        return 8;
    }

    @Override
    public void copyNormalizedKey(MemorySegment target, int offset, int len) {
        // see IntValue for an explanation of the logic
        if (len == 8) {
            // default case, full normalized key
            target.putLongBigEndian(offset, value - Long.MIN_VALUE);
        } else if (len <= 0) {
        } else if (len < 8) {
            long value = this.value - Long.MIN_VALUE;
            for (int i = 0; len > 0; len--, i++) {
                target.put(offset + i, (byte) (value >>> ((7 - i) << 3)));
            }
        } else {
            target.putLongBigEndian(offset, value - Long.MIN_VALUE);
            for (int i = 8; i < len; i++) {
                target.put(offset + i, (byte) 0);
            }
        }
    }
}
