package eu.stratosphere.types;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;

import java.io.IOException;

public interface CopyableValue<T> extends Value {

    int getBinaryLength();

    void copyTo(T target);

    void copy(DataInputView source, DataOutputView target) throws IOException;
}
