package eu.stratosphere.types;

public interface Key extends Value, Comparable<Key> {

    int hashCode();

    boolean equals(Object other);
}
