package eu.stratosphere.api.common.io.statistics;

public interface BaseStatistics {

    long SIZE_UNKNOW = -1;

    long NUM_RECORDS_UNKNOW = -1;

    float AVG_RECORDS_UNKNOW = -1;

    long getTotalInputSize();

    long getNumberOfRecords();

    float getAverageRecordWidth();
}
