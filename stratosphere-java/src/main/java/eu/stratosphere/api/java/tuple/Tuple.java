package eu.stratosphere.api.java.tuple;

/**
 * 元组
 *
 * @author yanpengfei
 * @date 2020/12/24
 **/
public abstract class Tuple {

    public abstract  <T> T getField(int pos);

    public abstract  <T> void setField(T value, int pos);

}
