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

    public void setString(String key, String value) {
        setStringInternal(key, value);
    }

    private void setStringInternal(String key, String value) {
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        }
        if (value == null) {
            throw new NullPointerException("Value must not be null.");
        }

        synchronized (this.confData) {
            this.confData.put(key, value);
        }
    }

    public void setInteger(String key, Integer value) {
        setStringInternal(key, String.valueOf(value));
    }

    public void setBoolean(String key, Boolean value) {
        setStringInternal(key, Boolean.toString(value));
    }

    public String getString(String key, String defaultValue) {
        String value = getStringInternal(key);
        return value == null ? defaultValue : value;
    }

    private String getStringInternal(String key) {
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        }
        synchronized (this.confData) {
            return confData.get(key);
        }
    }

    public int getInteger(String key, int defaultValue) {
        String value = getStringInternal(key);
        if (value == null) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    public long getLong(String key, long defaultValue) {
        String value = getStringInternal(key);
        if (value == null) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }
}
