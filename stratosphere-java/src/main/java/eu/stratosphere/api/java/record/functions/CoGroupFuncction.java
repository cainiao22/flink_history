package eu.stratosphere.api.java.record.functions;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericCoGrouper;
import eu.stratosphere.types.Record;
import eu.stratosphere.utils.Collector;

import java.util.Iterator;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/01/03 22:35
 */
public abstract class CoGroupFuncction extends AbstractFunction implements GenericCoGrouper<Record, Record, Record> {

    @Override
    public abstract void coGroup(Iterator<Record> records1, Iterator<Record> records2, Collector<Record> out) throws Exception;


    public void combineFirst(Iterator<Record> records, Collector<Record> collector){
        throw new UnsupportedOperationException();
    }

    public void combineSecond(Iterator<Record> records, Collector<Record> collector){
        throw new UnsupportedOperationException();
    }
}
