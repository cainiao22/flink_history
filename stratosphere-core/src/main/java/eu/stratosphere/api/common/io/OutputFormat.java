package eu.stratosphere.api.common.io;

import eu.stratosphere.configuration.Configuration;

import java.io.IOException;
import java.io.Serializable;

/**
 * 输出类
 *
 * @author yanpengfei
 * @date 2020/12/15
 **/
public interface OutputFormat<IT> extends Serializable {

    void configure(Configuration paramters);

    void open(int taskNumber) throws IOException;

    void writeRecord(IT record) throws IOException;

    void close() throws IOException;

}
