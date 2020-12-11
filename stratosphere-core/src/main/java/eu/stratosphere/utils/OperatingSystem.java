package eu.stratosphere.utils;

/**
 * @author ：yanpengfei
 * @date ：2020/12/10 6:08 下午
 * @description：TODO
 */
public enum OperatingSystem {
    LINUX,
    WINDOWS,
    MAC_OS,
    FREE_BSD,
    UNKNOWN;

    public static final OperatingSystem os = readFromProperties();

    public static OperatingSystem getCurrentOperatingSystem() {
        return os;
    }

    private static OperatingSystem readFromProperties() {
        String osName = System.getProperty(OS_KEY);
        if (osName.startsWith(LINUX_OS_PREFIX)) {
            return LINUX;
        }
        if (osName.startsWith(MAC_OS_PREFIX)) {
            return MAC_OS;
        }
        if (osName.startsWith(FREEBSD_OS_PREFIX)) {
            return FREE_BSD;
        }
        if (osName.startsWith(WINDOWS_OS_PREFIX)) {
            return WINDOWS;
        }
        return UNKNOWN;
    }

    public static boolean isWindows() {
        return getCurrentOperatingSystem() == WINDOWS;
    }


    private static final String OS_KEY = "os.name";

    /**
     * The expected prefix for Linux operating systems.
     */
    private static final String LINUX_OS_PREFIX = "Linux";

    /**
     * The expected prefix for Windows operating systems.
     */
    private static final String WINDOWS_OS_PREFIX = "Windows";

    /**
     * The expected prefix for Mac OS operating systems.
     */
    private static final String MAC_OS_PREFIX = "Mac";

    /**
     * The expected prefix for FreeBSD.
     */
    private static final String FREEBSD_OS_PREFIX = "FreeBSD";
}
