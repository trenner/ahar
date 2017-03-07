package de.tuberlin.cit.storageassistant.index;

import de.tuberlin.cit.storageassistant.DFSUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;

public class Index {
    private static final int INDEX_PARTITION_SIZE = 1000;

    private FileSystem fs;
    private Path harPath;

    private SortedMap<Integer, IndexFile> fileEntries = new TreeMap<>();
    private SortedMap<Integer, IndexDirectory> directoryEntries = new TreeMap<>();

    public Index(Path harPath, FileSystem fs) throws IOException {
        this.fs = fs;
        this.harPath = harPath;
        parseAllIndexEntries(harPath);
    }

    private void parseAllIndexEntries(Path harPath) throws IOException {
        List<IndexEntry> allIndexEntries = parseIndex(new Path(harPath, "_index"));
        for (IndexEntry entry : allIndexEntries) {
            if (entry.isFile())
                addEntry((IndexFile) entry);
            else
                addEntry((IndexDirectory) entry);
        }
    }

    private List<IndexEntry> parseIndex(Path indexPath) throws IOException {
        List<IndexEntry> parsedIndexEntries = new ArrayList<>();
        DFSUtils.readAsList(indexPath, fs).forEach(line -> parsedIndexEntries.add(IndexEntry.parse(line)));
        return parsedIndexEntries;
    }

    public void addEntry(IndexFile entry) {
        fileEntries.put(entry.getHarHash(), entry);
    }

    public void addEntry(IndexDirectory entry) {
        directoryEntries.merge(entry.getHarHash(), entry, IndexDirectory::combine);
    }

    public void writeToFile() {
        SortedSet<IndexEntry> sortedIndex = getAllEntries();
        try (FSDataOutputStream indexOutStream = fs.create(new Path(harPath, "_index"))) {
            IndexWriteObserver iWriteObserver = new IndexWriteObserver(INDEX_PARTITION_SIZE, harPath, fs);

            for (IndexEntry entry : sortedIndex) {
                indexOutStream.write(entry.getBytes());
                iWriteObserver.lineWritten(entry.getHarHash(), indexOutStream.getPos());
            }
            iWriteObserver.finalize(sortedIndex.last().getHarHash(), indexOutStream.getPos());
            indexOutStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private SortedSet<IndexEntry> getAllEntries() {
        SortedSet<IndexEntry> sortedIndex = new TreeSet<>();
        directoryEntries.forEach((hash, entry) -> sortedIndex.add(entry));
        fileEntries.forEach((hash, entry) -> sortedIndex.add(entry));
        return sortedIndex;
    }

    public boolean containsEntryFor(FileStatus fileStatus) {
        return containsEntryForHash(DFSUtils.getHarHash(fileStatus));
    }

    public boolean containsEntryForHash(int harHash) {
        return fileEntries.containsKey(harHash) || directoryEntries.containsKey(harHash);
    }

    public Integer numberOfDirectoryEntries() {
        return directoryEntries.size();
    }

    public Integer numberOfFileEntries() {
        return fileEntries.size();
    }
}
