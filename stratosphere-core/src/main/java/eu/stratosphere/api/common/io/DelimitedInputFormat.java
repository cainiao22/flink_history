package eu.stratosphere.api.common.io;

import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Charsets;
import com.sun.tools.javac.util.Assert;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;

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

    private transient int overLimit;

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

    }
}
