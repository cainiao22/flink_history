package eu.stratosphere.api.common.io.statistics;

public interface BaseStatistics {

    long SIZE_UNKNOWN = -1;

    long NUM_RECORDS_UNKNOWN = -1;

    float AVG_RECORDS_UNKNOWN = -1.0f;

    long getTotalInputSize();

    long getNumberOfRecords();

    float getAverageRecordWidth();
}
