package eu.stratosphere.api.common.operators;

import eu.stratosphere.api.common.operators.util.FieldSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CompilerHints {

    private float avgRecordsEmittedPerSubCall = -1.0f;

    private float avgBytePerRecord = -1.0f;

    private Map<FieldSet, Long> distinctCounts = new HashMap<FieldSet, Long>();

    private Map<FieldSet, Float> avgNumRecordsPerDistinctFields = new HashMap<FieldSet, Float>();

    private Set<FieldSet> uniqueFields;

    public void setAvgBytePerRecord(float avgBytePerRecord) {
        if (avgBytePerRecord < 0) {
            throw new RuntimeException("Average Bytes per Record must be  >= 0!");
        }
        this.avgBytePerRecord = avgBytePerRecord;
    }

    public float getAvgBytePerRecord() {
        return avgBytePerRecord;
    }

    public void setAvgRecordsEmittedPerSubCall(float avgRecordsEmittedPerSubCall) {
        if (avgRecordsEmittedPerSubCall < 0) {
            throw new RuntimeException("Average Number of Emitted Records per Function Call must be >= 0!");
        }
        this.avgRecordsEmittedPerSubCall = avgRecordsEmittedPerSubCall;
    }

    public float getAvgRecordsEmittedPerSubCall() {
        return avgRecordsEmittedPerSubCall;
    }

    public long getDistinctCount(FieldSet fieldSet) {
        Long estimate = distinctCounts.get(fieldSet);
        if (estimate == null) {
            estimate = -1L;
        }
        return estimate;
    }

    public void setDistinctCount(FieldSet fieldSet, long count) {
        if (count < 0) {
            throw new IllegalArgumentException("Cardinality must be >= 0!");
        }
        this.distinctCounts.put(fieldSet, count);
    }

    public Map<FieldSet, Long> getDistinctCounts() {
        return distinctCounts;
    }

    public float getAvgNumRecordsPerDistinctField(FieldSet columnSet) {
        Float avgNumberRecords = avgNumRecordsPerDistinctFields.get(columnSet);
        if (avgNumberRecords == null) {
            avgNumberRecords = -1.0f;
        }
        return avgNumberRecords;
    }

    public Map<FieldSet, Float> getAvgNumRecordsPerDistinctFields() {
        return avgNumRecordsPerDistinctFields;
    }

    public void setAvgNumRecordsPerDistinctFields(FieldSet fieldSet, float avgNumRecords) {
        if (avgNumRecords < 0) {
            throw new IllegalArgumentException("Average Number of Values per distinct Values must be >= 0");
        }
        this.avgNumRecordsPerDistinctFields.put(fieldSet, avgNumRecords);
    }

    public Set<FieldSet> getUniqueFields() {
        return uniqueFields;
    }

    public void addUniqueField(FieldSet fieldSet) {
        if (this.uniqueFields == null) {
            this.uniqueFields = new HashSet<FieldSet>();
        }
        this.uniqueFields.add(fieldSet);
    }

    public void addUniqueField(int field) {
        this.addUniqueField(new FieldSet(field));
    }

    public void addUniqueFields(Set<FieldSet> uniqueFieldSets) {
        if (this.uniqueFields == null) {
            this.uniqueFields = new HashSet<FieldSet>();
        }
        this.uniqueFields.addAll(uniqueFieldSets);
    }

    public void clearUniqueFields() {
        this.uniqueFields = null;
    }

    protected void copyFrom(CompilerHints source) {
        this.avgRecordsEmittedPerSubCall = source.avgRecordsEmittedPerSubCall;
        this.avgBytePerRecord = source.avgBytePerRecord;
        this.distinctCounts.putAll(source.distinctCounts);
        this.avgNumRecordsPerDistinctFields.putAll(source.avgNumRecordsPerDistinctFields);

        // ??? 为何要清空
        if (source.uniqueFields != null && source.uniqueFields.size() > 0) {
            if (this.uniqueFields == null) {
                this.uniqueFields = new HashSet<FieldSet>();
            } else {
                this.uniqueFields.clear();
            }
            this.uniqueFields.addAll(source.uniqueFields);
        }
    }
}
