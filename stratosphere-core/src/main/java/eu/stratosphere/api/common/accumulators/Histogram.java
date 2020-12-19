package eu.stratosphere.api.common.accumulators;

import com.google.common.collect.Maps;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2020/12/18
 **/
public class Histogram implements Accumulator<Integer, Map<Integer, Integer>> {

    private static final long serialVersionUID = 1L;

    private Map<Integer, Integer> hashMap = Maps.newHashMap();

    @Override
    public void add(Integer value) {
        Integer current = hashMap.get(value);
        Integer newValue = value;
        if (current != null) {
            newValue = current + newValue;
        }
        hashMap.put(value, newValue);
    }

    @Override
    public Map<Integer, Integer> getLocalValue() {
        return hashMap;
    }

    @Override
    public void resetLocal() {
        hashMap.clear();
    }

    @Override
    public void merge(Accumulator<Integer, Map<Integer, Integer>> other) {
        other.getLocalValue().forEach((key, value) -> {
            hashMap.merge(key, value, Integer::sum);
        });
    }

    @Override
    public void read(DataInput input) throws IOException {
        final int size = input.readInt();
        for (int i = 0; i < size; i++) {
            hashMap.put(input.readInt(), input.readInt());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(hashMap.size());
        for (Entry<Integer, Integer> kv : hashMap.entrySet()) {
            out.writeInt(kv.getKey());
            out.writeInt(kv.getValue());
        }
    }
}
