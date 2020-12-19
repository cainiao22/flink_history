package eu.stratosphere.types;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2020/12/19 18:54
 */
public class DoubleValue implements Key, CopyableValue<DoubleValue> {

    private static final long serialVersionUID = 1L;

    private double value;

    public DoubleValue() {
        this.value = 0;
    }

    public DoubleValue(double value) {
        this.value = value;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public int getBinaryLength() {
        return 8;
    }

    @Override
    public void copyTo(DoubleValue target) {
        target.setValue(this.value);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.write(source, 8);
    }

    @Override
    public void read(DataInput input) throws IOException {
        this.value = input.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.value);
    }

    @Override
    public int compareTo(Key o) {
        if (!(o instanceof DoubleValue))
            throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to DoubleValue!");

        final double other = ((DoubleValue) o).value;

        return Double.compare(this.value, other);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DoubleValue that = (DoubleValue) o;
        return Double.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode() {
        long temp = Double.doubleToLongBits(value);
        return 31 + (int)(temp ^ temp >>> 31);
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }
}
