package eu.stratosphere.api.common.functions;

import eu.stratosphere.configuration.Configuration;

import java.io.IOException;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2020/12/18
 **/
public abstract class AbstractFunction implements Function {

    private transient RuntimeContext runtimeContext;

    @Override
    public final void setRuntimeContext(RuntimeContext runtimeContext) {
        if (this.runtimeContext == null) {
            this.runtimeContext = runtimeContext;
        }
        throw new IllegalStateException("Error: The runtime context has already been set.");
    }

    @Override
    public final RuntimeContext getRuntimeContext() {
        if (this.runtimeContext != null) {
            return this.runtimeContext;
        }
        throw new IllegalStateException("Error: The runtime context has not been set.");
    }

    public IterationRuntimeContext getIterationRuntimeContext() {
        if (this.runtimeContext == null) {
            throw new IllegalStateException("Error: The runtime context has not been set.");
        } else if (this.runtimeContext instanceof IterationRuntimeContext) {
            return (IterationRuntimeContext) this.runtimeContext;
        } else {
            throw new IllegalStateException("This stub is not part of an iteration step function.");
        }
    }

    @Override
    public void open(Configuration paramters) {

    }

    @Override
    public void close() throws IOException {
        
    }
}
