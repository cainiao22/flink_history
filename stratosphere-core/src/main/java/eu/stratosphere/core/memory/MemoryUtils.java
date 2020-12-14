package eu.stratosphere.core.memory;

import java.nio.ByteOrder;
import sun.misc.Unsafe;

/**
 * @author ：yanpengfei
 * @date ：2020/12/14 9:57 上午
 * @description ：
 */
public class MemoryUtils {

    public static sun.misc.Unsafe UNSAFE = Unsafe.getUnsafe();
    public static final ByteOrder NATIVE_BYTE_ORDER = getByteOrder();

    private static ByteOrder getByteOrder(){
        byte[] bytes = new byte[8];
        long value = 0x1234567890abcdefL;
        UNSAFE.putLong(bytes, UNSAFE.arrayBaseOffset(byte[].class), value);
        int lower = bytes[0] & 0xff;
        int higher = bytes[7] & 0xff;
        if(lower == 0x12 && higher == 0xef){
            return ByteOrder.BIG_ENDIAN;
        }
        if(lower == 0xef && higher == 0x12){
            return ByteOrder.LITTLE_ENDIAN;
        }

        throw new RuntimeException("Unrecognized byte order.");

    }
}
