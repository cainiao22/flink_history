package eu.stratosphere.types;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Integer类型的包装类
 *
 * @author yanpengfei
 * @date 2020/12/22
 **/
public class IntValue implements NormalizableKey, CopyableValue<IntValue> {

    private int value;

    public IntValue() {
        this.value = 0;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public int getMaxNormalizedKeyLen() {
        return 4;
    }

    @Override
    public void copyNormalizedKey(MemorySegment target, int offset, int len) {
        if (len == 4) {
            target.putIntBigEndian(this.value - Integer.MIN_VALUE, offset);
        } else if (len < 4) {
            int value = this.value - Integer.MIN_VALUE;
            for (int i = 0; i < len; i++) {
                target.put(offset + i, (byte) ((value >>> ((3 - i) << 3)) & 0xff));
            }
        } else {
            target.putIntBigEndian(this.value - Integer.MIN_VALUE, offset);
            for (int i = 4; i < len; i++) {
                target.put(offset + i, (byte) 0);
            }
        }
    }

    @Override
    public void read(DataInput input) throws IOException {
        this.value = input.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(value);
    }

    @Override
    public int compareTo(Key key) {
        if (!(key instanceof IntValue)) {
            throw new ClassCastException(
                "Cannot compare " + key.getClass().getName() + " to IntValue!");
        }

        final int other = ((IntValue) key).value;

        return Integer.compare(this.value, other);
    }

    @Override
    public int getBinaryLength() {
        return 4;
    }

    @Override
    public void copyTo(IntValue target) {
        target.setValue(this.value);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.write(source, 4);
    }
}
