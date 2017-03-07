package de.tuberlin.cit.storageassistant.part;

import de.tuberlin.cit.storageassistant.index.IndexFile;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PartFile {
    private FileSystem fs;
    private FileStatus partFileStatus;
    private Long partFileSize; // original partfile + all added files
    private List<FileStatus> filesToAdd = new ArrayList<>();
    private Long blockSize;

    public PartFile(FileStatus partFileStatus, Long blockSize, FileSystem fs) {
        this.fs = fs;
        this.blockSize = blockSize;
        this.partFileStatus = partFileStatus;
        this.partFileSize = partFileStatus.getLen();
    }

    public boolean canFit(FileStatus fileStatus) {
        long spaceLeftInBlock = blockSize - (partFileSize %blockSize);
        return spaceLeftInBlock >= fileStatus.getLen();
    }

    public IndexFile addInputFile(FileStatus fileStatus) {
        filesToAdd.add(fileStatus);
        long positionFileWasAdded = partFileSize;
        partFileSize += fileStatus.getLen();
        return new IndexFile(fileStatus, positionFileWasAdded, this.getName());
    }

    private String getName() {
        return partFileStatus.getPath().getName();
    }

    public void writeToFile() throws IOException {
        FSDataInputStream inputStream;
        FSDataOutputStream outputStream = fs.append(partFileStatus.getPath());
        for (FileStatus fileStatus : filesToAdd) {
            inputStream = fs.open(fileStatus.getPath());
            IOUtils.copyBytes(inputStream, outputStream, (int) fileStatus.getLen());
            inputStream.close();
        }
        outputStream.close();
    }
}

