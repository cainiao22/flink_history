package eu.stratosphere.api.java.record.functions;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericJoiner;
import eu.stratosphere.types.Record;
import eu.stratosphere.utils.Collector;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/26 23:46
 */
public abstract class JoinFunction extends AbstractFunction implements GenericJoiner<Record, Record, Record> {

    @Override
    public abstract void join(Record value1, Record value2, Collector<Record> out) throws Exception;
}
