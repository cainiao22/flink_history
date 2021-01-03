package eu.stratosphere.api.common.io;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.core.fs.FSDataInputStream;
import eu.stratosphere.core.fs.FileInputSplit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/01/03 22:55
 */
public abstract class FileInputFormat<OT> implements InputFormat<OT, FileInputSplit> {

    private static final Log LOG = LogFactory.getLog(FileInputFormat.class);

    private static final long serialVersionUID = 1L;

    private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;

    static final long DEFAULT_OPENING_TIMEOUT;

    static {
        final long to = GlobalConfiguration.getLong(ConfigConstants.FS_STREAM_OPENING_TIMEOUT_KEY,
                ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT);
        if (to < 0) {
            LOG.error("Invalid timeout value for filesystem stream opening: " + to + ". Using default value of " +
                    ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT);
            DEFAULT_OPENING_TIMEOUT = ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT;
        } else if (to == 0) {
            DEFAULT_OPENING_TIMEOUT = Long.MAX_VALUE;
        } else {
            DEFAULT_OPENING_TIMEOUT = to;
        }
    }

    protected transient FSDataInputStream stream;

    protected transient long splitStart;

    protected transient long splitLength;
}
