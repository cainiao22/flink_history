package eu.stratosphere.configuration;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/01/03 23:14
 */
public class ConfigConstants {

    public static final String FS_STREAM_OPENING_TIMEOUT_KEY = "taskmanager.runtime.fs_timeout";

    public static final int DEFAULT_FS_STREAM_OPENING_TIMEOUT = 0;

    public static final String DELIMITED_FORMAT_MAX_LINE_SAMPLES_KEY = "compiler.delimited-informat.max-line-samples";

    public static final String DELIMITED_FORMAT_MIN_LINE_SAMPLES_KEY = "compiler.delimited-informat.min-line-samples";

    /**
     * The maximum length of a single sampled record before the sampling is aborted.
     */
    public static final String DELIMITED_FORMAT_MAX_SAMPLE_LENGTH_KEY = "compiler.delimited-informat.max-sample-len";

    /**
     * The default maximum number of line samples taken by the delimited input format.
     */
    public static final int DEFAULT_DELIMITED_FORMAT_MAX_LINE_SAMPLES = 10;

    /**
     * The default minimum number of line samples taken by the delimited input format.
     */
    public static final int DEFAULT_DELIMITED_FORMAT_MIN_LINE_SAMPLES = 2;

    /**
     * The default maximum sample length before sampling is aborted (2 MiBytes).
     */
    public static final int DEFAULT_DELIMITED_FORMAT_MAX_SAMPLE_LEN = 2 * 1024 * 1024;


}
