package eu.stratosphere.configuration;

import eu.stratosphere.core.fs.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Configuration implements IOReadableWritable {

    private final Map<String, String> confData = new HashMap<String, String>();

    @Override
    public void read(DataInput input) throws IOException {
        synchronized (confData) {
            int numberOfProperties = input.readInt();
            for (int i = 0; i < numberOfProperties; i++) {
                String key = StringRecord.readString(input);
                String value = StringRecord.readString(input);
                confData.put(key, value);
            }
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        synchronized (confData) {
            int numberOfProperties = confData.size();
            out.writeInt(numberOfProperties);
            final Iterator<String> it = this.confData.keySet().iterator();
            while (it.hasNext()) {
                String key = it.next();
                StringRecord.writeString(out, key);
                StringRecord.writeString(out, confData.get(key));
            }
        }
    }
}
