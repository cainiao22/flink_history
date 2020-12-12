package eu.stratosphere.core.fs.local;

import eu.stratosphere.core.fs.BlockLocation;

/**
 * 感觉像测试用的
 */
public class LocalBlockLocation implements BlockLocation {

    private final long length;

    private final String[] hosts;

    public LocalBlockLocation(final String host, final long length) {
        this.hosts = new String[]{host};
        this.length = length;
    }

    @Override
    public String[] getHosts() {
        return this.hosts;
    }

    @Override
    public long getOffset() {
        return 0;
    }

    @Override
    public long getLength() {
        return this.length;
    }

    @Override
    public int compareTo(BlockLocation blockLocation) {
        return 0;
    }
}
