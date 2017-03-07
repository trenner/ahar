package de.tuberlin.cit.storageassistant.index;

import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by joh-mue on 13/02/17.
 */
public class IndexWriteObserver {
    private ArrayList<MasterIndexEntry> masterIndexEntries = new ArrayList<>();
    private int indexPartitionSize;
    private Path harPath;
    private FileSystem fs;
    private int linesWritten = 0;
    private int startHash = 0;
    private long startPos = 0;

    public IndexWriteObserver(int indexPartitionSize, Path harPath, FileSystem fs) {
        this.indexPartitionSize = indexPartitionSize;
        this.harPath = harPath;
        this.fs = fs;
    }

    public void lineWritten(int harHash, long streamPositionAfterWrite) {
        linesWritten++;
        if (linesWritten == indexPartitionSize) { // every 1000 indexes we report status
            MasterIndexEntry newMasterIndexEntry = createMasterIndexEntry(harHash, streamPositionAfterWrite);
            masterIndexEntries.add(newMasterIndexEntry);
        }
    }

    public void finalize(int harHash, long endOfIndexStreamPosition) {
        MasterIndexEntry newMasterIndexEntry = createMasterIndexEntry(harHash, endOfIndexStreamPosition);
        masterIndexEntries.add(newMasterIndexEntry);

        try {
            writeMasterindexToFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeMasterindexToFile() throws IOException {
        FSDataOutputStream masterindexOutStream = fs.create(new Path(harPath, "_masterindex"));
        masterindexOutStream.write("3 \n".getBytes()); // the first line contains the har-version
        for (MasterIndexEntry masterIndexEntry : masterIndexEntries) {
            masterindexOutStream.write(masterIndexEntry.getBytes());
        }
        masterindexOutStream.close();
    }

    public MasterIndexEntry createMasterIndexEntry(int lastHash, long endPosition) {
        MasterIndexEntry masterIndexEntry = new MasterIndexEntry(startHash, lastHash, startPos, endPosition);
        resetObserver(lastHash, endPosition);
        return masterIndexEntry;
    }

    private void resetObserver(int lastHash, long endPosition) {
        startHash = lastHash;
        startPos = endPosition;
        this.linesWritten = 0;
    }


}
