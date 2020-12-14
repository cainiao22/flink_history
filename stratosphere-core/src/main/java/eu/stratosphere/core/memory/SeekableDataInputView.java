package eu.stratosphere.core.memory;

public interface SeekableDataInputView extends DataInputView {

    void setReadPosition(long position);
}
