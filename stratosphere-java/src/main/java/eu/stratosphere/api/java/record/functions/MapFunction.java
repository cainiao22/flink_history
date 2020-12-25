package eu.stratosphere.api.java.record.functions;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericMapper;
import eu.stratosphere.types.Record;
import eu.stratosphere.utils.Collector;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2020/12/24
 **/
public class MapFunction extends AbstractFunction implements GenericMapper<Record, Record> {

    @Override
    public void map(Record record, Collector<Record> out) throws Exception {

    }
}
