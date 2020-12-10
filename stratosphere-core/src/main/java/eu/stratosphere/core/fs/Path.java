package eu.stratosphere.core.fs;

import eu.stratosphere.core.io.StringRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author ：yanpengfei
 * @date ：2020/12/9 10:47 上午
 * @description：
 */
public class Path implements IOReadableWritable, Serializable {

    private static final long serialVersionUID = 1L;

    public static final String SEPARATOR = "/";

    public static final char SEPARATOR_CHAR = '/';

    public static final String CUR_DIR = ".";

    private URI uri;

    public Path(String schema, String authority, String path) {
        checkPathArg(path);
        initialize(schema, authority, path);
    }

    private void initialize(String schema, String authority, String path) {
        try {
            this.uri = new URI(schema, authority, path, null, null);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private void checkPathArg(String path) {
        assert path != null && path.length() > 0;
    }

    private String normalizePath(String path) {
        path = path.replace("//", SEPARATOR);
        path = path.replace("\\", SEPARATOR);

        return path;
    }

    public Path(Path parent, Path child) {
        URI parentUri = parent.uri;
        String parentPath = parentUri.getPath();
        if (!"".equals(parentPath) && !SEPARATOR.equals(parentPath)) {
            try {
                parentUri = new URI(parentUri.getScheme(), parentUri.getAuthority(), parentPath + SEPARATOR,
                        null, null);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }

        String childPath = child.uri.getPath();
        //主要目的是将子目录的绝对路径改成相对目录
        if (childPath.startsWith(SEPARATOR)) {
            child = new Path(child.uri.getScheme(), child.uri.getAuthority(), child.uri.getPath().substring(1));
        }

        final URI resolvedUri = parentUri.resolve(child.uri);
        initialize(resolvedUri.getScheme(), resolvedUri.getAuthority(), normalizePath(resolvedUri.getPath()));
    }

    /**
     * 从流中读取path数据
     *
     * @param input
     * @throws IOException
     */
    @Override
    public void read(DataInput input) throws IOException {
        boolean isNotNull = input.readBoolean();
        if (isNotNull) {
            String schema = StringRecord.readString(input);
            String userInfo = StringRecord.readString(input);
            String host = StringRecord.readString(input);
            int port = input.readInt();
            String path = StringRecord.readString(input);
            String query = StringRecord.readString(input);
            String fragment = StringRecord.readString(input);

            try {
                this.uri = new URI(schema, userInfo, host, port, path, query, fragment);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (this.uri == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            StringRecord.writeString(out, uri.getScheme());
            StringRecord.writeString(out, uri.getRawAuthority());
            StringRecord.writeString(out, uri.getHost());
            out.writeInt(uri.getPort());
            StringRecord.writeString(out, uri.getPath());
            StringRecord.writeString(out, uri.getQuery());
            StringRecord.writeString(out, uri.getFragment());
        }
    }
}
