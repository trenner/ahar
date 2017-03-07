package de.tuberlin.cit.storageassistant.index;

import org.apache.hadoop.fs.FileStatus;

public class IndexFile extends IndexEntry {
    private String part;
    private Long offset;
    private Long length;
    private Long time;
    private String rights;
    private String user;
    private String group;

    private String name;

    public IndexFile(FileStatus fileStatus, long offset, String part) {
        String qualifiedPath = getPathRelativeToRootDirectory(fileStatus);
        this.path = qualifiedPath;
        this.type = IndexEntry.TYPE_FILE;
        this.part = part;
        this.offset = offset;
        this.length = fileStatus.getLen();
        this.time = fileStatus.getModificationTime();
        this.rights = new Short(fileStatus.getPermission().toShort()).toString();
        this.user = fileStatus.getOwner();
        this.group = fileStatus.getGroup();
        this.name = fileStatus.getPath().getName();
    }

    protected IndexFile(String line) {
        String[] entrySet = line.split(" |\\+"); // \(^_^)\

        this.path = entrySet[0];
        this.type = IndexEntry.TYPE_FILE;
        this.part = entrySet[2];
        this.offset = new Long(entrySet[3]);
        this.length = new Long(entrySet[4]);
        this.time = new Long(entrySet[5]);
        this.rights = entrySet[6];
        this.user = entrySet[7];
        this.group = entrySet[8];
    }

    private String getPathRelativeToRootDirectory(FileStatus fileStatus) {
        return fileStatus.getPath().toString().replaceFirst("hdfs:\\/\\/.+:\\d{4,6}", "");
    }

    @Override
    public String toString() {
        return path.replace("/", "%2F") + " " + type + " " + part + " " + offset + " " + length + " " + time
                + "+" + rights + "+" + user + "+" + group;
    }

    @Override
    public boolean isFile() {
        return true;
    }

    @Override
    public Long getOffset() {
        return offset;
    }

    @Override
    public Long getLength() {
        return length;
    }

    @Override
    public String getName() {
        return name;
    }
}
