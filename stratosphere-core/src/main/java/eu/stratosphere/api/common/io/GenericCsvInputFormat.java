package eu.stratosphere.api.common.io;

import java.io.IOException;
import java.util.ArrayList;

import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.types.Value;
import eu.stratosphere.types.parser.FieldParser;
import eu.stratosphere.utils.InstantiationUtil;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2021/1/14
 **/
public abstract class GenericCsvInputFormat<OT> extends DelimitedInputFormat<OT> {
    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unchecked")
    private static final Class<? extends Value>[] EMPTY_TYPES = new Class[0];

    private static final boolean[] EMPTY_INCLUDED = new boolean[0];

    private static final char DEFAULT_FIELD_DELIMITER = ',';

    private transient FieldParser<Value>[] fieldParsers;

    private Class<? extends Value>[] fieldTypes = EMPTY_TYPES;

    private boolean[] fieldIncluded = EMPTY_INCLUDED;

    private char fieldDelim = DEFAULT_FIELD_DELIMITER;

    private boolean lenient = false;

    public GenericCsvInputFormat() {
        this(DEFAULT_FIELD_DELIMITER);
    }

    public GenericCsvInputFormat(char fieldDelimiter) {
        setFieldDelim(fieldDelimiter);
    }

    public GenericCsvInputFormat(Class<? extends Value>... fields) {
        this(DEFAULT_FIELD_DELIMITER, fields);
    }

    public GenericCsvInputFormat(char fieldDelimiter, Class<? extends Value>... fields) {
        setFieldDelim(fieldDelimiter);
        setFieldTypes(fields);
    }

    public Class<? extends Value>[] getFieldTypes() {
        if (fieldIncluded.length == fieldTypes.length) {
            return fieldTypes;
        }
        Class<? extends Value>[] types = new Class[this.fieldIncluded.length];
        for (int i = 0, k = 0; i < fieldTypes.length; i++) {
            if (fieldIncluded[i]) {
                types[k++] = fieldTypes[i];
            }
        }
        return types;
    }

    public void setFieldTypesArray(Class<? extends Value>[] fieldTypes) {
        setFieldTypes(fieldTypes);
    }

    public void setFieldTypes(Class<? extends Value>... fieldTypes) {
        if (fieldTypes == null) {
            throw new IllegalArgumentException("Field types must not be null.");
        }

        this.fieldIncluded = new boolean[fieldTypes.length];
        ArrayList<Class<? extends Value>> types = new ArrayList<Class<? extends Value>>();

        // check if we support parsers for these types
        for (int i = 0; i < fieldTypes.length; i++) {
            Class<? extends Value> type = fieldTypes[i];

            if (type != null) {
                if (FieldParser.getParserForType(type) == null) {
                    throw new IllegalArgumentException(
                            "The type '" + type.getName() + "' is not supported for the CSV input format.");
                }
                types.add(type);
                fieldIncluded[i] = true;
            }
        }
        this.fieldTypes = types.toArray(new Class[types.size()]);
    }

    public int getNumberOfFieldsTotal() {
        return this.fieldIncluded.length;
    }

    public int getNumberOfNonNullFields() {
        return this.fieldTypes.length;
    }

    public char getFieldDelim() {
        return fieldDelim;
    }

    public void setFieldDelim(char fieldDelim) {
        if (fieldDelim > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("The field delimiter must be an ASCII character.");
        }

        this.fieldDelim = fieldDelim;
    }

    public boolean isLenient() {
        return lenient;
    }

    public void setLenient(boolean lenient) {
        this.lenient = lenient;
    }

    public FieldParser<? extends Value>[] getFieldParsers() {
        return fieldParsers;
    }

    @Override
    public void open(FileInputSplit split) throws IOException {
        super.open(split);

        FieldParser<Value>[] parsers = new FieldParser[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            Class<? extends FieldParser<? extends Value>> parserClass = FieldParser.getParserForType(fieldTypes[i]);
            if (parserClass == null) {
                throw new RuntimeException("No parser available for type '" + fieldTypes[i].getName() + "'.");
            }
            FieldParser<Value> parser =
                    (FieldParser<Value>) InstantiationUtil.instantiate(parserClass, FieldParser.class);
            parsers[i] = parser;
        }
        this.fieldParsers = parsers;
    }

    protected boolean parseRecord(Value[] valueHolders, byte[] bytes, int offset, int numBytes) throws ParseException {
        boolean[] fieldIncluded = this.fieldIncluded;
        int startPos = offset;
        int limit = startPos + numBytes;
        for (int field = 0, output = 0; field < fieldIncluded.length; field++) {
            if (startPos > limit) {
                if (isLenient()) {
                    return false;
                } else {
                    throw new ParseException("Row too short: " + new String(bytes, offset, numBytes));
                }
            }

            if (fieldIncluded[field]) {
                FieldParser<Value> parser = fieldParsers[output];
                Value val = valueHolders[output];
                startPos = parser.parseField(bytes, startPos, limit, fieldDelim, val);
                if(startPos == -1) {
                    if (isLenient()) {
                        return false;
                    } else {
                        throw new ParseException("Row too short: " + new String(bytes, offset, numBytes));
                    }
                }
                output ++;
            } else {
                int skipCnt = 1;
                while(!fieldIncluded[field + skipCnt]){
                    skipCnt ++;
                }
                startPos = skipFields(bytes, startPos, limit,fieldDelim, skipCnt);
                if (startPos >= 0) {
                    field += (skipCnt - 1);
                }
                else {
                    String lineAsString = new String(bytes, offset, numBytes);
                    throw new ParseException("Line could not be parsed: " + lineAsString);
                }
            }
        }

        return true;
    }

    protected int skipFields(byte[] bytes, int startPos, int limit, char delimiter, int skipCnt){
        while(startPos < limit && skipCnt > 0){
            if(bytes[startPos] == delimiter){
                skipCnt --;
            }
            startPos ++;
        }
        if(skipCnt == 0){
            return startPos;
        }
        return -1;
    }
}
