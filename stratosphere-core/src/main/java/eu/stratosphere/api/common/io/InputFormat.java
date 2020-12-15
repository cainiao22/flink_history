package eu.stratosphere.api.common.io;

import eu.stratosphere.api.common.io.statistics.BaseStatistics;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.InputSplit;

import java.io.IOException;
import java.io.Serializable;

public interface InputFormat<OT, T extends InputSplit> extends Serializable {

    void configure(Configuration parameters);

    BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException;

    T[] createInputSplit(int minNumSplits) throws IOException;

    Class<T> getInputSplitType();

    void open(T split) throws IOException;

    boolean reachedEnd() throws IOException;

    boolean nextRecord(OT record) throws IOException;

    void close() throws IOException;
}
