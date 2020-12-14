package eu.stratosphere.core.memory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MemorySegment {

    private static final boolean CHECKED = true;

    protected byte[] memory;

    protected ByteBuffer wrapper;

    public MemorySegment(byte[] memory) {
        this.memory = memory;
    }

    public final boolean isFreed() {
        return this.memory == null;
    }

    public final int size() {
        return this.memory.length;
    }

    public ByteBuffer wrap(int offset, int length) {
        if (offset > this.memory.length || offset + length > this.memory.length) {
            throw new IndexOutOfBoundsException();
        }

        if (this.wrapper == null) {
            this.wrapper = ByteBuffer.wrap(memory, offset, length);
        } else {
            this.wrapper.position(offset);
            this.wrapper.limit(length);
        }

        return this.wrapper;
    }

    public final byte get(int idx) {
        return this.memory[idx];
    }

    public final void set(int idx, byte b) {
        this.memory[idx] = b;
    }

    public final void get(int index, byte[] dst) {
        get(index, dst, 0, dst.length);
    }

    public final void get(int index, byte[] dst, int offset, int length) {
        System.arraycopy(this.memory, index, dst, offset, length);
    }

    public final void put(int index, byte[] src) {
        put(index, src, 0, src.length);
    }

    public final void put(int index, byte[] src, int offset, int length) {
        System.arraycopy(src, index, this.memory, offset, length);
    }

    public final boolean getBoolean(int index) {
        return this.memory[index] != 0;
    }

    public final void setBoolean(int index, boolean value) {
        this.memory[index] = (byte) (value ? 1 : 0);
    }

    /**
     * 相当于占用了两个字节
     *
     * @param index
     * @return
     */
    public final char getChar(int index) {
        return (char) (this.memory[index] & 0xff << 8 | this.memory[index + 1] & 0xff);
    }

    public final void putChar(int index, char value) {
        this.memory[index] = (byte) (value >> 8);
        this.memory[index + 1] = (byte) value;
    }

    public final short getShort(int index) {
        return (short) (this.memory[index] & 0xff << 8 | this.memory[index + 1]);
    }

    public final void putShort(int index, short value) {
        this.memory[index] = (byte) (value >> 8);
        this.memory[index + 1] = (byte) value;
    }

    public final int getInt(int index) {
        if (CHECKED) {
            if (index < memory.length - 4) {
                return MemoryUtils.UNSAFE.getInt(memory, BASE_OFFSET + index);
            } else {
                throw new IndexOutOfBoundsException();
            }
        }
        return MemoryUtils.UNSAFE.getInt(memory, BASE_OFFSET + index);
    }

    public final int getIntLittleEndian(int index) {
        if (LITTLE_ENDIAN) {
            return get(index);
        }
        return Integer.reverseBytes(get(index));
    }

    public final int getIntBigEndian(int index) {
        if (LITTLE_ENDIAN) {
            return Integer.reverseBytes(get(index));
        }
        return get(index);
    }

    public final void putInt(int index, int value) {
        if (CHECKED) {
            if (index < memory.length - 4) {
                MemoryUtils.UNSAFE.putInt(memory, BASE_OFFSET + index, value);
            } else {
                throw new IndexOutOfBoundsException();
            }
        }
        MemoryUtils.UNSAFE.putInt(memory, BASE_OFFSET + index, value);
    }

    public final void putIntLittleEndian(int index, int value) {
        if (LITTLE_ENDIAN) {
            putInt(index, value);
        } else {
            putInt(index, Integer.reverseBytes(value));
        }
    }

    public final void putIntBigEndian(int index, int value) {
        if (!LITTLE_ENDIAN) {
            putInt(index, value);
        } else {
            putInt(index, Integer.reverseBytes(value));
        }
    }

    public final long getLong(int index) {
        if (CHECKED) {
            if (index < memory.length - 8) {
                return MemoryUtils.UNSAFE.getLong(memory, BASE_OFFSET + index);
            }
            throw new IndexOutOfBoundsException();
        }
        return MemoryUtils.UNSAFE.getLong(memory, BASE_OFFSET + index);
    }

    public final long getLongLittleEndian(int index) {
        if (LITTLE_ENDIAN) {
            return getLong(index);
        }
        return Long.reverseBytes(getLong(index));
    }

    public final long getLongBigEndian(int index) {
        if (!LITTLE_ENDIAN) {
            return getLong(index);
        }
        return Long.reverseBytes(getLong(index));
    }

    public final void putLong(int index, long value) {
        if (CHECKED) {
            if (index < memory.length - 8) {
                MemoryUtils.UNSAFE.putLong(memory, BASE_OFFSET + index, value);
            } else {
                throw new IndexOutOfBoundsException();
            }
        }
        MemoryUtils.UNSAFE.putLong(memory, BASE_OFFSET + index, value);
    }

    public final void putLongLittleEndian(int index, long value) {
        if (LITTLE_ENDIAN) {
            putLong(index, value);
        } else {
            putLong(index, Long.reverseBytes(value));
        }
    }

    public final void putLongBigEndian(int index, long value) {
        if (LITTLE_ENDIAN) {
            putLong(index, Long.reverseBytes(value));
        } else {
            putLong(index, value);
        }
    }

    public final float getFloat(int index) {
        return Float.intBitsToFloat(getInt(index));
    }

    public final float getFloatLittleEndian(int index) {
        return Float.intBitsToFloat(getIntLittleEndian(index));
    }

    public final float getFloatBigEndian(int index) {
        return Float.intBitsToFloat(getIntBigEndian(index));
    }

    public final void putFloat(int index, float value) {
        putInt(index, Float.floatToRawIntBits(value));
    }

    public final void putFloatLittleEndian(int index, float value) {
        putIntLittleEndian(index, Float.floatToRawIntBits(value));
    }

    public final void putFloatBigEndian(int index, float value) {
        putIntBigEndian(index, Float.floatToRawIntBits(value));
    }

    public final double getDouble(int index) {
        return Double.longBitsToDouble(getLong(index));
    }

    public final double getDoubleLittleEndian(int index) {
        return Double.longBitsToDouble(getLongLittleEndian(index));
    }

    public final double getDoubleBigEndian(int index) {
        return Double.longBitsToDouble(getLongBigEndian(index));
    }

    public final void putDouble(int index, double value) {
        putLong(index, Double.doubleToRawLongBits(value));
    }

    public final void putDoubleLittleEndian(int index, double value) {
        putLongLittleEndian(index, Double.doubleToRawLongBits(value));
    }

    public final void putDoubleBigEndian(int index, double value) {
        putLongBigEndian(index, Double.doubleToRawLongBits(value));
    }

    public final void get(DataOutput out, int offset, int length) throws IOException {
        out.write(memory, offset, length);
    }

    public final void put(DataInput in, int offset, int length) throws IOException {
        in.readFully(memory, offset, length);
    }

    public void get(int offset, ByteBuffer target, int numBytes) {
        target.get(memory, offset, numBytes);
    }

    public void put(int offset, ByteBuffer source, int numBytes) {
        source.get(memory, offset, numBytes);
    }

    public final void copyTo(int offset, MemorySegment target, int targetOffset, int numBytes) {
        System.arraycopy(this.memory, offset, target.memory, targetOffset, numBytes);
    }

    public static int compare(MemorySegment seg1, MemorySegment seg2, int offset1, int offset2, int len) {
        int var = 0;
        for (int i = 0; i < len && (var = seg1.get(offset1 + i) & 0xff - seg2.get(offset2 + i) & 0xff) == 0; i++) ;
        return var;
    }

    public static final void swapBytes(MemorySegment seg1, MemorySegment seg2, byte[] tempBuffer, int offset1, int offset2, int len) {
        // system arraycopy does the boundary checks anyways, no need to check extra
        System.arraycopy(seg1.memory, offset1, tempBuffer, 0, len);
        System.arraycopy(seg2.memory, offset2, seg1.memory, offset1, len);
        System.arraycopy(tempBuffer, 0, seg2.memory, offset2, len);
    }

    private static final int BASE_OFFSET = MemoryUtils.UNSAFE.arrayBaseOffset(byte[].class);

    private static final boolean LITTLE_ENDIAN = MemoryUtils.NATIVE_BYTE_ORDER == ByteOrder.LITTLE_ENDIAN;
}
