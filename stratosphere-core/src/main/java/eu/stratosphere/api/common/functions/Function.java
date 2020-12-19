package eu.stratosphere.api.common.functions;

import eu.stratosphere.configuration.Configuration;
import java.io.IOException;

public interface Function {

    void open(Configuration paramters);

    void close() throws IOException;

    RuntimeContext getRuntimeContext();

    void setRuntimeContext(RuntimeContext runtimeContext);
}
