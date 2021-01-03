package eu.stratosphere.api.java.record.functions;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericMapper;
import eu.stratosphere.types.Record;
import eu.stratosphere.utils.Collector;

/**
 * map function
 *
 * @author yanpengfei
 * @date 2020/12/24
 **/
public abstract class MapFunction extends AbstractFunction implements GenericMapper<Record, Record> {

    @Override
    public abstract void map(Record record, Collector<Record> out) throws Exception;
}
