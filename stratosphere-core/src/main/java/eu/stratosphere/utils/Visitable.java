package eu.stratosphere.utils;

/**
 * @author ：yanpengfei
 * @date ：2020/12/9 10:32 上午
 * @description：
 */
public interface Visitable<T extends Visitable<T>> {

    void accept(Visitor<T> visitor);
}
