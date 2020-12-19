package eu.stratosphere.api.common.functions;

import eu.stratosphere.utils.Collector;

public interface GenericMapper<T, O> extends Function {

    void map(T record, Collector<O> out) throws Exception;
}
