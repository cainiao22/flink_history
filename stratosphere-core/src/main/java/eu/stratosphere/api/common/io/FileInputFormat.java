package eu.stratosphere.api.common.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.api.common.io.statistics.BaseStatistics;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.core.fs.BlockLocation;
import eu.stratosphere.core.fs.FSDataInputStream;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;

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

    /**
     * The config parameter which defines the input file path.
     */
    private static final String FILE_PARAMETER_KEY = "pact.input.file.path";

    /**
     * The config parameter which defines the number of desired splits.
     */
    private static final String DESIRED_NUMBER_OF_SPLITS_PARAMETER_KEY = "pact.input.file.numsplits";

    /**
     * The config parameter for the minimal split size.
     */
    private static final String MINIMAL_SPLIT_SIZE_PARAMETER_KEY = "pact.input.file.minsplitsize";

    /**
     * The config parameter for the opening timeout in milliseconds.
     */
    private static final String INPUT_STREAM_OPEN_TIMEOUT_KEY = "pact.input.file.timeout";


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


    protected Path filePath;

    protected long minSplitSize = 0;

    protected int numSplits = -1;

    protected long openTimeout = DEFAULT_OPENING_TIMEOUT;

    public Path getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        if (filePath == null) {
            throw new IllegalArgumentException("File path may not be null.");
        }
        if (filePath.isEmpty()) {
            setFilePath(new Path());
        } else {
            setFilePath(new Path(filePath));
        }

    }

    public void setFilePath(Path filePath) {
        if (filePath == null) {
            throw new IllegalArgumentException("File path may not be null.");
        }
        this.filePath = filePath;
    }

    public long getMinSplitSize() {
        return minSplitSize;
    }

    public void setMinSplitSize(long minSplitSize) {
        if (minSplitSize < 0) {
            throw new IllegalArgumentException("the minSplitSize must > 0");
        }
        this.minSplitSize = minSplitSize;
    }

    public int getNumSplits() {
        return numSplits;
    }

    public void setNumSplits(int numSplits) {
        if (numSplits < -1 || numSplits == 0) {
            throw new IllegalArgumentException("The desired number of splits must be positive or -1 (= don't care).");
        }
        this.numSplits = numSplits;
    }

    public long getOpenTimeout() {
        return openTimeout;
    }

    public void setOpenTimeout(long openTimeout) {
        if (openTimeout < 0) {
            throw new IllegalArgumentException(
                    "The timeout for opening the input splits must be positive or zero (= infinite).");
        }
        this.openTimeout = openTimeout;
    }

    public long getSplitStart() {
        return splitStart;
    }

    public long getSplitLength() {
        return splitLength;
    }

    @Override
    public void configure(Configuration parameters) {
        String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
        if (filePath != null) {
            try {
                this.setFilePath(filePath);
            } catch (RuntimeException e) {
                throw new RuntimeException(
                        "Could not create a valid URI from the given file path name: " + e.getMessage());
            }
        } else if (this.filePath == null) {
            throw new IllegalArgumentException("File path was not specified in input format, or configuration.");
        }

        // get the number of splits
        int desiredSplits = parameters.getInteger(DESIRED_NUMBER_OF_SPLITS_PARAMETER_KEY, -1);
        if (desiredSplits != -1) {
            if (desiredSplits == 0 || desiredSplits < -1) {
                this.numSplits = -1;
                LOG.warn("Ignoring invalid parameter for number of splits: " + desiredSplits);
            } else {
                this.numSplits = desiredSplits;
            }
        }
        // get the minimal split size
        long minSplitSize = parameters.getLong(MINIMAL_SPLIT_SIZE_PARAMETER_KEY, -1);
        if (minSplitSize != -1) {
            if (minSplitSize < 0) {
                this.minSplitSize = 0;
                LOG.warn("Ignoring invalid parameter for minimal split size (requires a positive value): "
                        + minSplitSize);
            } else {
                this.minSplitSize = minSplitSize;
            }
        }

        long openTimeout = parameters.getLong(INPUT_STREAM_OPEN_TIMEOUT_KEY, -1);
        if (openTimeout != -1) {
            if (openTimeout < 0) {
                this.openTimeout = DEFAULT_OPENING_TIMEOUT;
                LOG.warn(
                        "Ignoring invalid parameter for stream opening timeout (requires a positive value or "
                                + "zero=infinite): "
                                + openTimeout);
            } else if (openTimeout == 0) {
                this.openTimeout = Long.MAX_VALUE;
            } else {
                this.openTimeout = openTimeout;
            }
        }
    }

    @Override
    public FileBaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        FileBaseStatistics cachedFileStats = null;
        if (cachedStatistics instanceof FileBaseStatistics) {
            cachedFileStats = (FileBaseStatistics) cachedStatistics;
        }
        try {
            Path path = this.filePath;
            FileSystem fileSystem = FileSystem.get(path.toUri());
            return getFileStatus(cachedFileStats, filePath, fileSystem, new ArrayList<>(1));
        } catch (IOException ioex) {
            LOG.warn("Could not determine statistics for file '" + this.filePath + "' due to an io error: "
                    + ioex.getMessage());
        } catch (Throwable t) {
            LOG.error("Unexpected problen while getting the file statistics for file '" + this.filePath + "': "
                    + t.getMessage(), t);
        }
        // no statistics available
        return null;
    }

    protected FileBaseStatistics getFileStatus(FileBaseStatistics cachedStats, Path filePath, FileSystem fs,
            ArrayList<FileStatus> files) throws IOException {
        FileStatus fileStatus = fs.getFileStatus(filePath);
        long lastModTime = fileStatus.getModificationTime();
        if (fileStatus.isDir()) {
            FileStatus[] fss = fs.listStatus(filePath);
            files.ensureCapacity(fss.length);
            for (FileStatus s : fss) {
                if (!s.isDir()) {
                    files.add(s);
                    lastModTime = Math.max(lastModTime, s.getModificationTime());
                }
            }
        } else {
            files.add(fileStatus);
        }
        if (cachedStats != null && cachedStats.getLastModificationTime() > lastModTime) {
            return cachedStats;
        }
        long len = 0;
        for (FileStatus file : files) {
            len += file.getLen();
        }
        // sanity check
        if (len <= 0) {
            len = BaseStatistics.SIZE_UNKNOWN;
        }
        return new FileBaseStatistics(lastModTime, len, BaseStatistics.AVG_RECORDS_UNKNOWN);
    }

    @Override
    public Class<FileInputSplit> getInputSplitType() {
        return FileInputSplit.class;
    }

    @Override
    public FileInputSplit[] createInputSplit(int minNumSplits) throws IOException {
        if (minNumSplits < 1) {
            throw new IllegalArgumentException("Number of input splits has to be at least 1.");
        }

        minNumSplits = Math.max(minNumSplits, this.numSplits);
        Path path = this.filePath;
        List<FileInputSplit> inputSplits = new ArrayList<>(minNumSplits);
        List<FileStatus> files = new ArrayList<FileStatus>();
        long totalLength = 0;

        FileSystem fs = FileSystem.get(path.toUri());
        FileStatus pathFile = fs.getFileStatus(path);
        if (!accept(pathFile)) {
            throw new IOException("The given file does not pass the file-filter");
        }
        if (pathFile.isDir()) {
            FileStatus[] fss = fs.listStatus(path);
            for (FileStatus status : fss) {
                if (!status.isDir() && accept(status)) {
                    files.add(status);
                    totalLength += status.getLen();
                }
            }
        } else {
            files.add(pathFile);
            totalLength = pathFile.getLen();
        }

        long maxSplitSize = minNumSplits == 1 ? Long.MAX_VALUE : (totalLength + minNumSplits - 1) / minNumSplits;
        int splitNum = 0;
        for (FileStatus file : files) {
            long len = file.getLen();
            long blockSize = file.getBlockSize();
            long minSplitSize = this.minSplitSize;
            if (blockSize < minSplitSize) {
                LOG.warn("Minimal split size of " + this.minSplitSize + " is larger than the block size of " +
                        blockSize + ". Decreasing minimal split size to block size.");
                minSplitSize = blockSize;
            }
            long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
            long halfSplit = splitSize >>> 1;
            float maxBytesForLastSplit = splitSize * MAX_SPLIT_SIZE_DISCREPANCY;
            if (len > 0) {
                BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
                Arrays.sort(blocks);

                long bytesUnassigned = len;
                long position = 0;
                int blockIndex = 0;
                while (bytesUnassigned > maxBytesForLastSplit) {
                    blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
                    FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position, splitSize,
                            blocks[blockIndex].getHosts());
                    inputSplits.add(fis);
                    position += splitSize;
                    bytesUnassigned -= splitSize;
                }
                // assign the last split
                if (bytesUnassigned > 0) {
                    blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
                    final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position,
                            bytesUnassigned, blocks[blockIndex].getHosts());
                    inputSplits.add(fis);
                }
            } else {
                //有可能文件是空的 大小为0
                BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
                String[] hosts;
                if (blocks.length > 0) {
                    hosts = blocks[0].getHosts();
                } else {
                    hosts = new String[0];
                }
                final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), 0,
                        0, hosts);
                inputSplits.add(fis);
            }
        }

        return inputSplits.toArray(new FileInputSplit[0]);
    }

    private int getBlockIndexForPosition(BlockLocation[] blocks, long offset, long halfSplit, int startIndex) {
        for (int i = startIndex; i < blocks.length; i++) {
            long blockStart = blocks[i].getOffset();
            long blockEnd = blockStart + blocks[i].getLength();
            if (offset >= blockStart && offset < blockEnd) {
                //如果下一个block包含的数据多余当前的，那么就返回下一下
                if (i < blocks.length - 1 && blockEnd - offset < halfSplit) {
                    return i + 1;
                }
                return i;
            }
        }
        throw new IllegalArgumentException("The given offset is not contained in the any block.");
    }

    private boolean accept(FileStatus pathFile) {
        String name = pathFile.getPath().getName();
        return !name.startsWith("_") && !name.startsWith(".");
    }

    @Override
    public void open(FileInputSplit split) throws IOException {
        this.splitStart = split.getStart();
        this.splitLength = split.getLength();
        LOG.debug("Opening input split " + split.getPath() + " [" + this.splitStart + "," + this.splitLength + "]");


    }

    public static class FileBaseStatistics implements BaseStatistics {

        /**
         * 文件最后修改时间
         */
        protected final long fileModTime;
        /**
         * 文件大小 （单位bytes）
         */
        protected final long fileSize;
        /**
         * 一条记录的平均byte数
         */
        protected final float avgBytesPerRecord;


        public FileBaseStatistics(long fileModTime, long fileSize, float avgBytesPerRecord) {
            this.fileModTime = fileModTime;
            this.fileSize = fileSize;
            this.avgBytesPerRecord = avgBytesPerRecord;
        }

        public long getLastModificationTime() {
            return fileModTime;
        }


        @Override
        public long getTotalInputSize() {
            return fileSize;
        }

        @Override
        public long getNumberOfRecords() {
            if (fileSize == SIZE_UNKNOWN || avgBytesPerRecord == AVG_RECORDS_UNKNOWN) {
                return NUM_RECORDS_UNKNOWN;
            }
            return (long) Math.ceil(fileSize / avgBytesPerRecord);
        }

        @Override
        public float getAverageRecordWidth() {
            return this.avgBytesPerRecord;
        }

        @Override
        public String toString() {
            return "size=" + this.fileSize + ", recWidth=" + this.avgBytesPerRecord + ", modAt=" + this.fileModTime;
        }
    }

    public static class InputSplitOpenThread extends Thread {

        private FileInputSplit split;

        private long timeout;

        private volatile FSDataInputStream fdis;

        private volatile Throwable error;

        private volatile boolean aborted;

        public InputSplitOpenThread(FileInputSplit split, long timeout) {
            super("Transient InputSplit Opener");
            this.split = split;
            this.timeout = timeout;
        }

        @Override
        public void run() {
            try {
                FileSystem fs = FileSystem.get(split.getPath().toUri());
                fdis = fs.open(split.getPath());
                if(this.aborted){
                    final FSDataInputStream fdis = this.fdis;
                    this.fdis = null;
                    fdis.close();
                }
            } catch (Throwable t) {
                this.error = error;
            }
        }

        public FSDataInputStream waitForCompletion() throws InterruptedException {
            long start = System.currentTimeMillis();
            long remaining = timeout;
            try {
                this.join(remaining);
            } catch (InterruptedException e) {
                abortWait();
                throw e;
            }
            return this.fdis;
        }

        private void abortWait() {
            this.aborted = true;
            FSDataInputStream stream = fdis;
            fdis = null;
            if(stream != null){
                try {
                    stream.close();
                } catch (IOException e) {

                }
            }
        }
    }
}
