package eu.stratosphere.types.parser;

import eu.stratosphere.types.IntValue;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2021/1/14
 **/
public class DecimalTextIntParser extends FieldParser<IntValue> {
    /**
     *
     * @param bytes
     * @param startPos
     * @param limit
     * @param delim
     * @param field
     * @return 返回读完后下一个需要读取的位置
     */
    @Override
    public int parseField(byte[] bytes, int startPos, int limit, char delim, IntValue field) {
        long val = 0;
        boolean neg = false;
        if (bytes[startPos] == '-') {
            neg = true;
            startPos++;
        }
        for (int i = startPos; i < limit; i++) {
            if (bytes[i] == delim) {
                return valueSet(field, val, neg, i + 1);
            }
            if(bytes[i] < '0' || bytes[i] > '9'){
                return -1;
            }
            val = val * 10 + (bytes[i] - '0');
        }
        return valueSet(field, val, neg, limit);
    }

    private int valueSet(IntValue field, long val, boolean negative, int position) {
        if (negative) {
            if (val >= Integer.MIN_VALUE) {
                field.setValue((int) -val);
            } else {
                return -1;
            }
        } else {
            if (val <= Integer.MAX_VALUE) {
                field.setValue((int) val);
            } else {
                return -1;
            }
        }
        return position;
    }

    @Override
    public IntValue createValue() {
        return new IntValue();
    }
}
