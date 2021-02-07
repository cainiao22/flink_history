package eu.stratosphere.types.parser;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Value;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2021/1/14
 **/
public abstract class FieldParser<T extends Value> {

    public abstract int parseField(byte[] bytes, int offset, int limit, char delim, T field);

    public abstract T createValue();

    public static <T extends Value> Class<FieldParser<T>> getParserForType(Class<T> type) {
        Class<? extends FieldParser<?>> parser = PARSERS.get(type);
        if (parser == null) {
            return null;
        } else {
            @SuppressWarnings("unchecked")
            Class<FieldParser<T>> typedParser = (Class<FieldParser<T>>) parser;
            return typedParser;
        }
    }


    private static final Map<Class<? extends Value>, Class<? extends FieldParser<?>>> PARSERS =
            new HashMap<Class<? extends Value>, Class<? extends FieldParser<?>>>();

    static {
        PARSERS.put(IntValue.class, DecimalTextIntParser.class);
    }
}
