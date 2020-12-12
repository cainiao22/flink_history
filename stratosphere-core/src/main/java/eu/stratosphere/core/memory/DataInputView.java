package eu.stratosphere.core.memory;

import java.io.DataInput;
import java.io.IOException;

public interface DataInputView extends DataInput {

    void skipBytesToRead(int numBytes) throws IOException;
}
