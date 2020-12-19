package eu.stratosphere.utils;

public interface Collector<T> {

    void collect(T record);

    void close();
}
