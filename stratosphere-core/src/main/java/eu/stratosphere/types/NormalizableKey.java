package eu.stratosphere.types;

import eu.stratosphere.core.memory.MemorySegment;

/**
 * 基本类型的key
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/19 19:13
 */
public interface NormalizableKey extends Key {

    int getMaxNormalizedKeyLen();

    void copyNormalizedKey(MemorySegment target, int offset, int len);
}
