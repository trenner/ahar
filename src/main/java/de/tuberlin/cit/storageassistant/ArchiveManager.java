package de.tuberlin.cit.storageassistant;

import de.tuberlin.cit.storageassistant.index.Index;
import de.tuberlin.cit.storageassistant.part.PartFileManager;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;


public class ArchiveManager {
    private final static Logger log = Logger.getLogger(ArchiveManager.class);
    private FileSystem fs;
    private HadoopArchives hadoopArchives;
    private Configuration conf;

    /**
     * @param conf
     * @throws IOException
     */
    public ArchiveManager(Configuration conf) throws IOException {
        this.conf = conf;
        this.fs = FileSystem.get(conf);
        this.hadoopArchives = new HadoopArchives(conf);
    }

    /**
     * @param harPath
     * @param fileList
     * @return
     */
    public int createArchive(Path harPath, Path[] fileList) {
        return createArchive(harPath, fileList, 3);
    }

    /**
     * @param harPath
     * @param fileList
     * @param rep
     * @return
     */
    public int createArchive(Path harPath, Path[] fileList, int rep) {
        try {
            String[] args = prepareArguments(harPath, fileList, String.valueOf(rep));
            return ToolRunner.run(hadoopArchives, args);
        } catch (Exception e) {
            System.err.println("Caught Exception (Unable to createArchive): " + e.getMessage());
            return -1;
        }
    }

    private String[] prepareArguments(Path harPath, Path[] fileList, String replication) {
        Object[] args = new String[]{"-archiveName", harPath.getName(), "-p", "/", "-r", replication};
        for (Path path : fileList) {
            args = ArrayUtils.add(args, makeRelativeToRoot(path.toString()));
        }
        args = ArrayUtils.add(args, new String(harPath.getParent().toString() + "/"));
        return (String[]) args;
    }

    private String makeRelativeToRoot(String path) {
        if (path.charAt(0) == '/') {
            return path.substring(1);
        } else {
            return path;
        }
    }

    /**
     * @param srcPath
     * @param harPath
     * @throws IOException
     */
    public void addFileToArchive(Path srcPath, Path harPath) throws IOException {
        Path[] paths = {srcPath};
        addFilesToArchive(paths, harPath);
    }

    /**
     * @param srcPaths
     * @param harPath
     * @throws IOException
     */
    public void addFilesToArchive(Path[] srcPaths, Path harPath) throws IOException {
        Index index = new Index(harPath, fs);
        PartFileManager partFileManager = new PartFileManager(fs, harPath);
        partFileManager.addToPartFilesAndUpdateIndex(srcPaths, index);
        partFileManager.writeAllToFile();
        index.writeToFile();
    }
}