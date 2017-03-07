package de.tuberlin.cit.storageassistant.part;

import de.tuberlin.cit.storageassistant.index.Index;
import de.tuberlin.cit.storageassistant.index.IndexDirectory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PartFileManager {
    private List<PartFile> partFiles;
    private final Path harPath;
    private FileSystem fs;
    private Index index;
    private Long defaultHarBlockSize;

    public PartFileManager(FileSystem fs, Path harArchivePath) throws IOException {
        this.fs = fs;
        //TODO default block size for part is 512mb
        this.defaultHarBlockSize = fs.getFileStatus(harArchivePath.suffix("/part-0")).getBlockSize();
        this.harPath = harArchivePath;
        this.partFiles = loadPartFiles();
    }

    private ArrayList<PartFile> loadPartFiles() throws IOException {
        ArrayList<PartFile> partFiles = new ArrayList<>();
        FileStatus[] files = fs.listStatus(harPath);
        for (FileStatus fileStatus : files) {
            String fileName = fileStatus.getPath().getName();
            if (fileName.startsWith("part-")) {
                partFiles.add(createNewPartFile(fileStatus.getPath()));
            }
        }
        return partFiles;
    }

    public void addToPartFilesAndUpdateIndex(Path[] inputPaths, Index index) throws IOException {
        this.index = index;
        addParentDirectories(inputPaths);
        addToPartFilesAndIndex(getFileStatuses(inputPaths));
    }

    private void addParentDirectories(Path[] paths) throws IOException {
        for (Path path : paths)
            addParentDirectoriesRecursively(path);
    }

    private void addParentDirectoriesRecursively(Path path) throws IOException {
        if (!path.isRoot()) {
            Path parent = path.getParent();
            IndexDirectory newEntry = new IndexDirectory(fs.getFileStatus(parent));
            newEntry.addChild(path.getName());
            index.addEntry(newEntry);
            addParentDirectoriesRecursively(parent);
        }
    }

    private List<FileStatus> getFileStatuses(Path[] paths) throws IOException {
        List<FileStatus> result = new ArrayList<>();
        for (Path p : paths)
            result.add(fs.getFileStatus(p));
        return result;
    }

    private void addToPartFilesAndIndex(List<FileStatus> inputFileStatuses) throws IOException {
        for (FileStatus current : inputFileStatuses) {
            if (current.isDirectory()) {
                addDirectoryEntryToIndex(current);
                addToPartFilesAndIndex(getChildrenFromDirectory(current));
            } else if (current.isFile() && !index.containsEntryFor(current)) {
                addToPartFile(current);
            }
        }
    }

    private void addDirectoryEntryToIndex(FileStatus fileStatus) throws IOException {
        IndexDirectory newEntry = new IndexDirectory(fileStatus);
        newEntry.addChildren(getChildrenFromDirectory(fileStatus));
        index.addEntry(newEntry);
    }

    private List<FileStatus> getChildrenFromDirectory(FileStatus directoryFileStatus) throws IOException {
        List<FileStatus> children = new ArrayList<>();
        if (directoryFileStatus.isDirectory())
            Collections.addAll(children, fs.listStatus(directoryFileStatus.getPath()));
        return children;
    }

    private void addToPartFile(FileStatus fileStatus) throws IOException {
        for (PartFile partFile : partFiles) {
            if (partFile.canFit(fileStatus)) {
                index.addEntry(partFile.addInputFile(fileStatus));
                return;
            }
        } // if no appropriate partFile was found
        addToNewPartFile(fileStatus);
    }

    private void addToNewPartFile(FileStatus fileStatus) throws IOException {
        PartFile partFile = createNewPartFile(new Path(harPath, "part-" + partFiles.size()));
        index.addEntry(partFile.addInputFile(fileStatus));
        partFiles.add(partFile);
    }

    private PartFile createNewPartFile(Path path) throws IOException {
        fs.createNewFile(path);
        FileStatus partFileStatus = fs.getFileStatus(path);
        return new PartFile(partFileStatus, defaultHarBlockSize, fs);
    }

    public void writeAllToFile() throws IOException {
        for (PartFile partFile : partFiles)
            partFile.writeToFile();
    }
}