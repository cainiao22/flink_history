package eu.stratosphere.api.common.io;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Charsets;
import com.sun.tools.javac.util.Assert;

import eu.stratosphere.api.common.io.statistics.BaseStatistics;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;

/**
 * TODO 类描述
 *
 * @author yanpengfei
 * @date 2021/1/8
 **/
public abstract class DelimitedInputFormat<OT> extends FileInputFormat<OT> {

    private static final long serialVersionUID = 1L;

    // -------------------------------------- Constants -------------------------------------------

    /**
     * The log.
     */
    private static final Log LOG = LogFactory.getLog(DelimitedInputFormat.class);

    private static final int NUM_SAMPLES_UNDEFINED = -1;

    /**
     * The maximum number of line samples to be taken.
     */
    private static int DEFAULT_MAX_NUM_SAMPLES;

    /**
     * The minimum number of line samples to be taken.
     */
    private static int DEFAULT_MIN_NUM_SAMPLES;

    /**
     * The maximum size of a sample record before sampling is aborted. To catch cases where a wrong delimiter is given.
     */
    private static int MAX_SAMPLE_LEN;

    /**
     * The default read buffer size = 1MB.
     */
    private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 1024;

    static {
        loadGloablConfigParams();
    }

    private static final void loadGloablConfigParams() {
        int maxSamples = GlobalConfiguration.getInteger(ConfigConstants.DELIMITED_FORMAT_MAX_LINE_SAMPLES_KEY,
                ConfigConstants.DEFAULT_DELIMITED_FORMAT_MAX_LINE_SAMPLES);
        int minSamples = GlobalConfiguration.getInteger(ConfigConstants.DELIMITED_FORMAT_MIN_LINE_SAMPLES_KEY,
                ConfigConstants.DEFAULT_DELIMITED_FORMAT_MIN_LINE_SAMPLES);

        if (maxSamples < 0) {
            LOG.error("Invalid default maximum number of line samples: " + maxSamples + ". Using default value of " +
                    ConfigConstants.DEFAULT_DELIMITED_FORMAT_MAX_LINE_SAMPLES);
            maxSamples = ConfigConstants.DEFAULT_DELIMITED_FORMAT_MAX_LINE_SAMPLES;
        }
        if (minSamples < 0) {
            LOG.error("Invalid default minimum number of line samples: " + minSamples + ". Using default value of " +
                    ConfigConstants.DEFAULT_DELIMITED_FORMAT_MIN_LINE_SAMPLES);
            minSamples = ConfigConstants.DEFAULT_DELIMITED_FORMAT_MIN_LINE_SAMPLES;
        }

        DEFAULT_MAX_NUM_SAMPLES = maxSamples;

        if (minSamples > maxSamples) {
            LOG.error("Defaul minimum number of line samples cannot be greater the default maximum number " +
                    "of line samples: min=" + minSamples + ", max=" + maxSamples + ". Defaulting minumum to maximum.");
            DEFAULT_MIN_NUM_SAMPLES = maxSamples;
        } else {
            DEFAULT_MIN_NUM_SAMPLES = minSamples;
        }

        int maxLen = GlobalConfiguration.getInteger(ConfigConstants.DELIMITED_FORMAT_MAX_SAMPLE_LENGTH_KEY,
                ConfigConstants.DEFAULT_DELIMITED_FORMAT_MAX_SAMPLE_LEN);
        if (maxLen <= 0) {
            maxLen = ConfigConstants.DEFAULT_DELIMITED_FORMAT_MAX_SAMPLE_LEN;
            LOG.error("Invalid value for the maximum sample record length. Using defailt value of " + maxLen + '.');
        } else if (maxLen < DEFAULT_READ_BUFFER_SIZE) {
            maxLen = DEFAULT_READ_BUFFER_SIZE;
            LOG.warn("Increasing maximum sample record length to size of the read buffer (" + maxLen + ").");
        }
        MAX_SAMPLE_LEN = maxLen;
    }

    private transient byte[] readBuffer;

    private transient byte[] wrapBuffer;

    private transient int readPos;
    private transient int limit;

    private transient byte[] currBuffer;
    private transient int currPos;
    private transient int currLen;
    private transient int currOffset;

    private transient boolean overLimit;

    private transient boolean end;

    private byte[] delimiter = new byte[] {'\n'};

    private int lineLengthLimit = Integer.MAX_VALUE;

    private int bufferSize = -1;

    private int numLineSamples = NUM_SAMPLES_UNDEFINED;

    public byte[] getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(byte[] delimiter) {
        Assert.checkNonNull(delimiter, "Delimiter must not be null");
        this.delimiter = delimiter;
    }

    public void setDelimiter(char delimiter) {
        setDelimiter(String.valueOf(delimiter));
    }

    public void setDelimiter(String delimiter) {
        this.setDelimiter(delimiter, Charsets.UTF_8);
    }

    public void setDelimiter(String delimiter, String charsetName) {
        Assert.checkNull(charsetName, "charsetName must not be null");
        Charset charset = Charset.forName(charsetName);
        setDelimiter(delimiter, charset);
    }

    public void setDelimiter(String delimiter, Charset charset) {
        if (delimiter == null) {
            throw new IllegalArgumentException("Delimiter must not be null");
        }
        if (charset == null) {
            throw new IllegalArgumentException("Charset must not be null");
        }

        this.delimiter = delimiter.getBytes(charset);
    }

    public int getLineLengthLimit() {
        return lineLengthLimit;
    }

    public void setLineLengthLimit(int lineLengthLimit) {
        if (lineLengthLimit < 1) {
            throw new IllegalArgumentException("Line length limit must be at least 1.");
        }

        this.lineLengthLimit = lineLengthLimit;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        if (bufferSize < 1) {
            throw new IllegalArgumentException("Buffer size must be at least 1.");
        }

        this.bufferSize = bufferSize;
    }

    public int getNumLineSamples() {
        return numLineSamples;
    }

    public void setNumLineSamples(int numLineSamples) {
        if (numLineSamples < 0) {
            throw new IllegalArgumentException("Number of line samples must not be negative.");
        }

        this.numLineSamples = numLineSamples;
    }

    public abstract void readRecord(OT targer, byte[] bytes, int offset, int numByes);


    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);
        String delimString = parameters.getString(RECORD_DELIMITER, null);
        if (delimString != null) {
            String charsetName = parameters.getString(RECORD_DELIMITER_ENCODING, null);
            if (charsetName == null) {
                setDelimiter(delimString);
            } else {
                setDelimiter(delimString, charsetName);
            }
        }

        int samples = parameters.getInteger(NUM_STATISTICS_SAMPLES, 0);
        setNumLineSamples(samples);
    }

    @Override
    public FileBaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        FileBaseStatistics cachedFileStats =
                cachedStatistics instanceof FileBaseStatistics ? (FileBaseStatistics) cachedStatistics : null;
        FileSystem fs = FileSystem.get(this.filePath.toUri());
        ArrayList<FileStatus> allFiles = new ArrayList<>(1);
        FileBaseStatistics stats = getFileStatus(cachedFileStats, this.filePath, fs, allFiles);
        if (stats == null) {
            return null;
        }
        if (stats.getAverageRecordWidth() == BaseStatistics.AVG_RECORDS_UNKNOWN
                || stats.getTotalInputSize() == BaseStatistics.SIZE_UNKNOWN) {
            return stats;
        }
        int numSamples;
        if (this.numLineSamples != NUM_SAMPLES_UNDEFINED) {
            numSamples = this.numLineSamples;
        } else {
            int calcSamples = (int) (stats.getTotalInputSize() / 1024);
            numSamples = Math.min(DEFAULT_MAX_NUM_SAMPLES, Math.max(DEFAULT_MIN_NUM_SAMPLES, calcSamples));
        }
        if (numSamples == 0) {
            return stats;
        }
        if (numSamples < 0) {
            throw new RuntimeException("Error: Invalid number of samples: " + numSamples);
        }
        this.openTimeout = 10000;
        this.bufferSize = 4 * 1024;

        this.lineLengthLimit = MAX_SAMPLE_LEN;

        long offset = 0;
        long totalNumBytes = 0;

        int fileNum = 0;
        int samplesTaken = 0;
        long stepSize = stats.getTotalInputSize() / numSamples;

        while (samplesTaken < numSamples && fileNum < allFiles.size()) {
            FileStatus file = allFiles.get(fileNum);
            FileInputSplit split = new FileInputSplit(0, file.getPath(), offset, file.getLen() - offset, null);
            try {
                open(split);
                if (readLine()) {
                    totalNumBytes += this.currLen + delimiter.length;
                    samplesTaken++;
                }
            } finally {
                super.close();
            }

            offset += stepSize;
            while (fileNum < allFiles.size() && offset >= (file = allFiles.get(fileNum)).getLen()) {
                offset -= file.getLen();
                fileNum++;
            }
        }
        // we have the width, store it
        return new FileBaseStatistics(stats.getLastModificationTime(),
                stats.getTotalInputSize(), totalNumBytes / (float) samplesTaken);

    }

    @Override
    public void open(FileInputSplit split) throws IOException {
        super.open(split);
        if (this.bufferSize < 0) {
            this.bufferSize = DEFAULT_READ_BUFFER_SIZE;
        }
        if (this.readBuffer == null || this.readBuffer.length < this.bufferSize) {
            this.readBuffer = new byte[this.bufferSize];
        }
        if (this.wrapBuffer == null || this.wrapBuffer.length < 256) {
            this.wrapBuffer = new byte[256];
        }
        this.readPos = 0;
        this.limit = 0;
        this.overLimit = false;
        this.end = false;

        if (this.splitStart != 0) {
            this.stream.seek(this.splitStart);
            readLine();
            if (this.overLimit) {
                this.end = true;
            }
        } else {
            fillBuffer();
        }
    }

    private boolean readLine() throws IOException {
        if (this.stream == null || this.end) {
            return false;
        }
        int countInWrapBuffer = 0;
        int i = 0;
        while (true) {
            if (this.readPos >= this.limit) {
                if (!fillBuffer()) {
                    if (countInWrapBuffer > 0) {
                        setResult(this.wrapBuffer, 0, countInWrapBuffer);
                        return true;
                    }
                    return false;
                }
            }

            int startPos = this.readPos;
            int count = 0;
            while (this.readPos < this.limit && i < this.delimiter.length) {
                if (this.readBuffer[this.readPos++] == this.delimiter[i]) {
                    i++;
                } else {
                    i = 0;
                }
            }

            if (i == this.delimiter.length) {
                count = this.readPos - startPos - this.delimiter.length;
                if (this.wrapBuffer.length < count + countInWrapBuffer) {
                    byte[] n = new byte[count + countInWrapBuffer];
                    System.arraycopy(this.wrapBuffer, 0, n, 0, countInWrapBuffer);
                    this.wrapBuffer = n;
                }
                if (count > 0) {
                    System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, count);
                }
                setResult(this.wrapBuffer, 0, count + countInWrapBuffer);
                return true;
            } else {
                count = this.limit - startPos;
                if (count + countInWrapBuffer > this.lineLengthLimit) {
                    throw new IOException("The record length exceeded the maximum record length (" +
                            this.lineLengthLimit + ").");
                }

                if (this.wrapBuffer.length < count + countInWrapBuffer) {
                    byte[] n = new byte[count + countInWrapBuffer];
                    System.arraycopy(this.wrapBuffer, 0, n, 0, countInWrapBuffer);
                    this.wrapBuffer = n;
                }
                System.arraycopy(this.readBuffer, startPos, this.wrapBuffer, countInWrapBuffer, count);
                countInWrapBuffer += count;
            }
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return this.end;
    }

    @Override
    public boolean nextRecord(OT record) throws IOException {
        if (readLine()) {
            readRecord(record, this.currBuffer, this.currOffset, this.currLen);
            return true;
        } else {
            this.end = true;
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        this.wrapBuffer = null;
        this.readBuffer = null;
        super.close();
    }

    private final void setResult(byte[] buffer, int offset, int len) {
        this.currBuffer = buffer;
        this.currOffset = offset;
        this.currLen = len;
    }

    private boolean fillBuffer() throws IOException {
        int toRead = Math.min((int) this.splitLength, this.readBuffer.length);
        if (this.splitLength <= 0) {
            toRead = this.readBuffer.length;
            this.overLimit = true;
        }
        int read = this.stream.read(this.readBuffer, 0, toRead);
        if (read == -1) {
            this.stream.close();
            this.stream = null;
            return false;
        } else {
            this.splitLength -= read;
            this.readPos = 0;
            this.limit = read;
            return true;
        }
    }

    /**
     * The configuration key to set the record delimiter.
     */
    protected static final String RECORD_DELIMITER = "delimited-format.delimiter";

    /**
     * The configuration key to set the record delimiter encoding.
     */
    private static final String RECORD_DELIMITER_ENCODING = "delimited-format.delimiter-encoding";

    /**
     * The configuration key to set the number of samples to take for the statistics.
     */
    private static final String NUM_STATISTICS_SAMPLES = "delimited-format.numSamples";
}
