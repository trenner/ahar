package de.tuberlin.cit.storageassistant.index;

import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;

public abstract class IndexEntry implements Comparable<IndexEntry> {
    public static final String TYPE_FILE = "file";
    public static final String TYPE_DIRECTORY = "dir";

    protected String path;
    protected String type;
    protected Integer harHash;

    abstract public String toString();

    abstract public Long getOffset();

    abstract public Long getLength();

    abstract public boolean isFile();

    public static IndexEntry parse(String line) {
        String[] entrySet = line.split(" ", 3); // \(^_^)\
        if (entrySet[1].equals("file")) {
            return new IndexFile(line);
        } else {
            return new IndexDirectory(line);
        }
    }

    protected Integer calculateHarHash() {
        return HarFileSystem.getHarHash(new Path(path.replace("%2F", "/")));
    }

    public Integer getHarHash() {
        if (harHash == null) {
            this.harHash = calculateHarHash();
        }
        return harHash;
    }

    public byte[] getBytes() {
        return (toString() + "\n").getBytes();
    }

    public String getName() {
        return path.substring(path.lastIndexOf("/") + 1);
    }

    public String getPath() {
        return path;
    }

    public String getType() {
        return type;
    }

    @Override
    public int compareTo(IndexEntry entry) {
        return harHash.compareTo(entry.getHarHash());
    }
}
