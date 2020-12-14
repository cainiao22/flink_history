package eu.stratosphere.core.memory;

public interface MemorySegmentSource {

    MemorySegment nextSegment();
}
