package eu.stratosphere.api.common.operators.util;

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

    public int size(){
        return this.collection.size();
    }

    @Override
    public Iterator<Integer> iterator() {
        return this.collection.iterator();
    }


    protected Collection<Integer> initCollection() {
        return new HashSet<Integer>();
    }
}
