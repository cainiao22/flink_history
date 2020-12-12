package eu.stratosphere.core.memory;

import java.io.DataOutput;
import java.io.IOException;

public interface DataOutputView extends DataOutput {

    void skipBytesToWrite(int numBytes) throws IOException;

    void write(DataInputView source, int numBytes) throws IOException;
}
