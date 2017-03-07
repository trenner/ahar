package de.tuberlin.cit.storageassistant.index;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by joh-mue on 02/11/16.
 *
 * line examples in file
 * %2Finput%2Fsub0 dir 1478098068547+493+Johannes+supergroup 0 0 fileA.txt fileB.txt fileC.txt fileD.txt fileX-1.txt
 * %2Finput%2Fsub1 dir 1478098068700+493+Johannes+supergroup 0 0 fileA.txt fileB.txt fileC.txt fileD.txt fileX-1.txt
 * %2Finput%2Fsub2 dir 1478098068861+493+Johannes+supergroup 0 0 fileA.txt fileB.txt fileC.txt fileD.txt fileX-1.txt
 *
 * internal representation
 * /multiInput dir 1481466358383+493+Johannes+supergroup 0 0 dir3 dir2 dir1
 * /multiInput/dir1 dir 1481466358264+493+Johannes+supergroup 0 0 dir1-1
 * /multiInput/dir2 dir 1481466358361+493+Johannes+supergroup 0 0 dir2-1
 * /multiInput/dir3 dir 1481466358384+493+Johannes+supergroup 0 0 dir3-1
 * /multiInput/dir1/dir1-1 dir 1481466358342+493+Johannes+supergroup 0 0 file1-1-1.txt file1-1-2.txt file1-2.txt
 *
 */
public class IndexDirectory extends IndexEntry {
    private String line;
    private Long time;
    private String rights;
    private String user;
    private String group;

    private String name;
    private Set<String> children = new HashSet<>();

    public IndexDirectory(FileStatus fileStatus) {
        this.path = fileStatus.getPath().toString().replaceFirst("hdfs:\\/\\/.+:\\d{4,6}",""); // don't ask
        this.type = IndexEntry.TYPE_DIRECTORY;
        this.time = fileStatus.getModificationTime();
        this.rights = new Short(fileStatus.getPermission().toShort()).toString();
        this.user = fileStatus.getOwner();
        this.group = fileStatus.getGroup();
        this.name = fileStatus.getPath().getName();
    }

    protected IndexDirectory(String line) {
        String[] entrySet = line.split(" |\\+");
        this.line = line;
        this.path = entrySet[0].replace("%2F", "/");
        this.type = IndexEntry.TYPE_DIRECTORY;
        this.time = new Long(entrySet[2]);
        this.rights = entrySet[3];
        this.user = entrySet[4];
        this.group = entrySet[5];
        // items 6 and 7 are 0 for directories

        if (entrySet.length > 7) {
            for (int i = 8; i < entrySet.length; i++) {
                children.add(entrySet[i]);
            }
        }
    }

    @Override
    public String toString() {
        String line = path.replace("/","%2F") + " dir " + time + "+" + rights + "+" + user + "+" + group + " 0 0 ";
        for (String child: children) {
            line += child + " "; // TODO: get rid of leading '/'
        }
        return line;
    }

    @Override
    public boolean isFile() {
        return false;
    }

    @Override
    public Long getOffset() {
        return new Long(0);
    }

    @Override
    public Long getLength() {
        return new Long(0);
    }

    public void addChild(String childName) {
        children.add(childName);
    }

    public void addChildren(List<FileStatus> children) {
        for (FileStatus fileStatus : children) {
            addChild(fileStatus.getPath().getName());
        }
    }

    public Set<String> getChildren() {
        return children;
    }

    public IndexDirectory combine(IndexDirectory entry) {
        children.addAll(entry.getChildren());
        return this;
    }
}
