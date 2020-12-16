package eu.stratosphere.api.common.operators.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FieldList extends FieldSet {

    public FieldList() {
        super();
    }

    public FieldList(int columnIndex) {
        super();
        add(columnIndex);
    }

    public FieldList(int[] columnIndexes) {
        super();
        addAll(columnIndexes);
    }

    @Override
    public FieldList toFieldList() {
        return this;
    }

    @Override
    public boolean isValidSubset(FieldSet set) {
        if (set instanceof FieldList) {
            return (isValidSubset((FieldList) set));
        } else {
            return false;
        }
    }

    /**
     * 这里面的元素是按照一定顺序排列的，子集的顺序必须和父亲的一样，并且不可以跳过
     * @param list
     * @return
     */
    public boolean isValidSubset(FieldList list) {
        if(list.size() > this.size()){
            return false;
        }
        final List<Integer> myList = get();
        final List<Integer> theirList = list.get();
        for (int i = 0; i < theirList.size(); i++) {
            Integer myInt = myList.get(i);
            Integer theirInt = theirList.get(i);
            if (myInt.intValue() != theirInt.intValue()) {
                return false;
            }
        }
        return true;
    }

    public boolean isValidUnorderedPrefix(FieldSet set) {
        if (set.size() > size()) {
            return false;
        }

        List<Integer> list = get();
        for (int i = 0; i < set.size(); i++) {
            if (!set.contains(list.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Collection<Integer> initCollection() {
        return new ArrayList<Integer>();
    }

    @Override
    protected String getDescriptionPrefix() {
        return "[";
    }

    @Override
    protected String getDescriptionSuffix() {
        return "]";
    }

    public int get(Integer pos) {
        return get().get(pos);
    }

    private List<Integer> get() {
        return (List<Integer>) this.collection;
    }
}
