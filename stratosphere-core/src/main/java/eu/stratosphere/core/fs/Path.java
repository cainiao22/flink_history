package eu.stratosphere.core.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.utils.OperatingSystem;

/**
 * @author ：yanpengfei
 * @date ：2020/12/9 10:47 上午
 * @description： 这个path 相当于通常意义下的文件或文件夹了
 */
public class Path implements IOReadableWritable, Serializable {

    private static final long serialVersionUID = 1L;

    public static final String SEPARATOR = "/";

    public static final char SEPARATOR_CHAR = '/';

    public static final String CUR_DIR = ".";

    private URI uri;

    public Path(){

    }

    public Path(String schema, String authority, String path) {
        checkPathArg(path);
        initialize(schema, authority, path);
    }

    public Path(String pathString) {
        checkPathArg(pathString);
        if (hasWindowDriver(pathString, false)) {
            pathString += "/";
        }
        String scheme = null;
        String authority = null;

        int start = 0;
        // parse uri scheme, if any
        final int colon = pathString.indexOf(':');
        final int slash = pathString.indexOf('/');
        //完整uri 示例 http://username:password@host:8080/directory/file?query#fragment
        if (colon != -1 && (slash == -1 || slash > colon)) {
            scheme = pathString.substring(0, colon);
            start = colon + 1;
        }

        if (pathString.startsWith("//", start) && pathString.length() - start > 2) {
            final int nextSlash = pathString.indexOf('/', start + 2);
            final int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
            authority = pathString.substring(start + 2, authEnd);
            start = authEnd;
        }

        final String path = pathString.substring(start, pathString.length());

        initialize(scheme, authority, path);
    }

    private boolean hasWindowDriver(String path, boolean slashed) {
        if (!OperatingSystem.isWindows()) {
            return false;
        }
        int start = slashed ? 1 : 0;

        return path.length() >= start + 2
                && (slashed ? path.charAt(0) == '/' : true)
                && path.charAt(start + 1) == ':'
                && ((path.charAt(start) >= 'A' && path.charAt(start) <= 'Z') || (path.charAt(start) >= 'a' && path
                .charAt(start) <= 'z'));
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

    public Path(Path parent, String child) {
        this(parent, new Path(child));
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

    public boolean isAbsolute() {
        int start = OperatingSystem.isWindows() ? 3 : 0;
        return this.uri.getPath().startsWith(SEPARATOR, start);
    }

    public Path makeQualified(FileSystem fs) {
        Path path = this;
        if (!isAbsolute()) {
            path = new Path(fs.getWorkingDirectory(), this);
        }

        URI pathUri = path.uri;

        String scheme = path.uri.getScheme();
        String authority = path.uri.getAuthority();

        if (scheme != null && (authority != null || fs.getUri().getAuthority() == null)) {
            return path;
        }
        if (scheme == null) {
            scheme = fs.getUri().getScheme();
        }
        if (authority == null) {
            if (fs.getUri().getAuthority() != null) {
                authority = fs.getUri().getAuthority();
            } else {
                authority = "";
            }
        }

        return new Path(scheme + ":" + "//" + authority + pathUri.getPath());
    }

    public URI toUri() {
        return this.uri;
    }

    public Path getParent() {
        String path = uri.getPath();
        int lastSlash = path.lastIndexOf(SEPARATOR_CHAR);
        int start = hasWindowDriver(path, true) ? 3 : 0;
        if (path.length() == start ||
                (lastSlash == start && path.length() == start + 1)) {
            return null;
        }

        String parent;
        if (lastSlash == -1) {
            parent = CUR_DIR;
        } else {
            final int end = hasWindowDriver(path, true) ? 3 : 0;
            parent = path.substring(0, lastSlash == end ? end + 1 : lastSlash);
        }
        return new Path(uri.getScheme(), uri.getAuthority(), parent);
    }

    public String getName() {
        String path = this.uri.getPath();
        int slash = path.lastIndexOf(SEPARATOR);
        return path.substring(slash + 1);
    }
}
