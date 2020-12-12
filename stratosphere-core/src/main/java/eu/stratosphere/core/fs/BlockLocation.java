package eu.stratosphere.core.fs;

public interface BlockLocation extends Comparable<BlockLocation> {

    String[] getHosts();

    long getOffset();

    long getLength();
}
