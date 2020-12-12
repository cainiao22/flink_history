package eu.stratosphere.core.memory;

import java.nio.ByteBuffer;

public class MemorySegment {

    private static final boolean CHECKED = true;

    protected byte[] memory;

    protected ByteBuffer wrapper;
}
