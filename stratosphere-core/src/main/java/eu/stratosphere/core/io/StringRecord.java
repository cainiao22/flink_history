package eu.stratosphere.core.io;

import eu.stratosphere.core.fs.IOReadableWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;

/**
 * @author ：yanpengfei
 * @date ：2020/12/9 3:54 下午
 * @description：TODO
 */
public class StringRecord implements IOReadableWritable {

    private static final ThreadLocal<CharsetEncoder> ENCODER_FACTORY = new ThreadLocal<CharsetEncoder>() {
        @Override
        protected CharsetEncoder initialValue() {
            return Charset.forName("UTF-8").newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
        }
    };

    private static final ThreadLocal<CharsetDecoder> DECODER_FACTORY = new ThreadLocal<CharsetDecoder>() {
        @Override
        protected CharsetDecoder initialValue() {
            return Charset.forName("UTF-8").newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
        }
    };

    private int hash;

    private static final byte[] EMPTY_BYTES = new byte[0];

    private byte[] bytes;

    /**
     * 这个指的是bytes数组中的有效字节的长度
     */
    private int length;

    public StringRecord() {
        this.bytes = EMPTY_BYTES;
        this.length = 0;
    }

    public StringRecord(String string) {
        set(string);
    }

    public void set(final String string) {
        try {
            ByteBuffer bytes = encode(string);
            this.bytes = bytes.array();
            this.length = bytes.limit();
            this.hash = 0;
        } catch (CharacterCodingException e) {
            e.printStackTrace();
        }
    }

    public void set(final byte[] utf8, final int start, final int len) {
        setCapacity(len, false);
        System.arraycopy(utf8, start, bytes, 0, len);
        this.length = len;
        this.hash = 0;
    }

    public void append(final byte[] utf8, final int start, final int len) {
        setCapacity(this.length + len, true);
        System.arraycopy(utf8, start, this.bytes, this.length, len);
        this.length += len;
        this.hash = 0;
    }

    private void setCapacity(final int len, final boolean keepData) {
        if (this.length < len) {
            byte[] newbytes = new byte[len];
            if (this.bytes != null && keepData) {
                System.arraycopy(this.bytes, 0, newbytes, 0, this.length);
            }
            this.bytes = newbytes;
        }
    }

    public static ByteBuffer encode(final String string) throws CharacterCodingException {
        return encode(string, true);
    }

    public static ByteBuffer encode(final String string, final boolean replace) throws CharacterCodingException {
        CharsetEncoder encoder = ENCODER_FACTORY.get();
        if (replace) {
            encoder.onUnmappableCharacter(CodingErrorAction.REPLACE)
                    .onMalformedInput(CodingErrorAction.REPLACE);
        }
        ByteBuffer bytes = encoder.encode(CharBuffer.wrap(string.toCharArray()));
        if (replace) {
            encoder.onUnmappableCharacter(CodingErrorAction.REPORT)
                    .onMalformedInput(CodingErrorAction.REPORT);
        }

        return bytes;
    }

    @Override
    public void read(DataInput input) throws IOException {
        int newLen = input.readInt();
        setCapacity(newLen, false);
        input.readFully(this.bytes, 0, newLen);
        this.length = newLen;
        this.hash = 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.length);
        out.write(bytes, 0, length);
    }

    public static void skip(final DataInput in) throws IOException {
        int skipLen = in.readInt();
        skipFully(in, skipLen);
    }

    public static void skipFully(final DataInput in, final int len) throws IOException {
        int cur = 0;
        int total = 0;
        //因为in.skipBytes 跳过的长度可能会小于预定长度，in的数据长度不够仅仅是其中的一种情况
        while (total < len && (cur = in.skipBytes(len - total)) > 0) {
            total += cur;
        }

        if (total < len) {
            throw new IOException("Not able to skip " + len + " bytes, possibly " + "due to end of input.");
        }
    }

    public void clear() {
        this.length = 0;
        this.hash = 0;
    }

    @Override
    public int hashCode() {
        int h = this.hash;
        if (h == 0 && this.length > 0) {
            for (int i = 0; i < this.length; i++) {
                h = 31 * h + bytes[i];
            }
        }
        this.hash = h;
        return h;
    }

    public static String decode(final byte[] utf8) throws CharacterCodingException {
        return decode(ByteBuffer.wrap(utf8), true);
    }

    public static String decode(final ByteBuffer utf8, final boolean replace) throws CharacterCodingException {
        CharsetDecoder decoder = DECODER_FACTORY.get();
        if (replace) {
            decoder.onUnmappableCharacter(CodingErrorAction.REPLACE)
                    .onMalformedInput(CodingErrorAction.REPLACE);
        }
        String string = decoder.decode(utf8).toString();
        if (replace) {
            decoder.onUnmappableCharacter(CodingErrorAction.REPORT)
                    .onMalformedInput(CodingErrorAction.REPORT);
        }

        return string;
    }

    public static String readString(final DataInput in) throws IOException {
        if (in.readBoolean()) {
            int len = in.readInt();
            if (len < 0) {
                throw new IOException("length of StringRecord is " + len);
            }
            byte[] bytes = new byte[len];
            in.readFully(bytes);
            return decode(bytes);
        }

        return null;
    }

    public static int writeString(final DataOutput out, final String utf8) throws IOException {
        int len = 0;
        //无论如何都必须写一个boolean值在第一位，避免读的时候异常
        if (utf8 != null) {
            out.writeBoolean(true);
            ByteBuffer byteBuffer = encode(utf8);
            len = byteBuffer.limit();
            out.writeInt(len);
            out.write(byteBuffer.array(), 0, len);
        } else {
            out.writeBoolean(false);
        }

        return len;
    }
}
