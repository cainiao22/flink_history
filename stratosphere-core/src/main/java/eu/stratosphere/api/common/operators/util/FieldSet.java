package eu.stratosphere.api.common.operators.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

public class FieldSet implements Iterable<Integer> {

    protected final Collection<Integer> collection;

    public FieldSet() {
        this.collection = initCollection();
    }

    public FieldSet(int columnIndex) {
        this();
        add(columnIndex);
    }

    public FieldSet(int[] columnIndexes) {
        this();
        this.addAll(columnIndexes);
    }

    public void add(int columnIndex) {
        this.collection.add(columnIndex);
    }

    public void addAll(int[] columnIndexes) {
        for (int columnIndex : columnIndexes) {
            this.add(columnIndex);
        }
    }

    public void addAll(Collection<Integer> columnIndexes) {
        this.collection.addAll(columnIndexes);
    }

    /**
     * 因为fieldSet实现的是iterator 所以增强for循环可以用
     *
     * @param fieldSet
     */
    public void addAll(FieldSet fieldSet) {
        for (Integer i : fieldSet) {
            this.add(i);
        }
    }

    public boolean contains(int columnIndex) {
        return this.collection.contains(columnIndex);
    }

    public int size() {
        return this.collection.size();
    }

    @Override
    public Iterator<Integer> iterator() {
        return this.collection.iterator();
    }

    public FieldList toFieldList() {
        int[] pos = toArray();
        Arrays.sort(pos);
        return new FieldList(pos);
    }

    public int[] toArray() {
        int[] a = new int[collection.size()];
        int i = 0;
        for (Integer col : collection) {
            a[i] = col;
        }
        return a;
    }

    public boolean isValidSubset(FieldSet sub) {
        if (sub.size() > this.size()) {
            return false;
        }
        //因为是iterator 所以可以直接增强for循环
        for (Integer col : sub) {
            if (this.collection.contains(col)) {
                return false;
            }
        }
        return true;
    }

    protected Collection<Integer> initCollection() {
        return new HashSet<Integer>();
    }

    // --------------------------------------------------------------------------------------------


    @Override
    public int hashCode() {
        return this.collection.hashCode();
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof FieldSet) {
            return this.collection.equals(((FieldSet) obj).collection);
        } else {
            return false;
        }
    }

    @Override
    public FieldSet clone() {
        FieldSet clone = new FieldSet();
        clone.addAll(this.collection);
        return clone;
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append(getDescriptionPrefix());
        for (Integer i : this.collection) {
            bld.append(i);
            bld.append(',');
            bld.append(' ');
        }
        if (this.collection.size() > 0) {
            bld.setLength(bld.length() - 2);
        }
        bld.append(getDescriptionSuffix());
        return bld.toString();
    }

    protected String getDescriptionPrefix() {
        return "(";
    }

    protected String getDescriptionSuffix() {
        return ")";
    }
}
