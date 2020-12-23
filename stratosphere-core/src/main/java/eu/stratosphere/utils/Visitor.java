package eu.stratosphere.utils;

/**
 * @author ：yanpengfei
 * @date ：2020/12/9 10:34 上午
 * @description：
 */
public interface Visitor<T extends Visitable> {

    boolean preVisit(T visitable);

    void postVisit(T visitable);

}
