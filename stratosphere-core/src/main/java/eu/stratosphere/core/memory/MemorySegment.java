package eu.stratosphere.core.memory;

import java.nio.ByteBuffer;

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

    public final short getShort(int index){
        return (short) (this.memory[index] & 0xff << 8 | this.memory[index + 1]);
    }

    public final void putShort(int index, short value){
        this.memory[index] = (byte) (value >> 8);
        this.memory[index + 1] = (byte) value;
    }

}
