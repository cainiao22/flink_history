package eu.stratosphere.core.memory;

public interface SeekableDataOutputView extends DataOutputView {

    void setWritePosition(long position);
}
