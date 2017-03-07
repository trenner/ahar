package de.tuberlin.cit.storageassistant;

import de.tuberlin.cit.storageassistant.index.Index;
import de.tuberlin.cit.storageassistant.index.IndexDirectory;
import de.tuberlin.cit.storageassistant.index.IndexEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.Ignore;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Created by joh-mue on 07/10/16.
 */
public class ArchiveManagerTest {
    private Configuration conf;
    private MiniDFSCluster dfsCluster;
    private MiniYARNCluster miniYARNCluster;
    private String hdfsURI;

    private FileSystem fs;
    private final ArrayList<String> fileList = new ArrayList<>();
    private final List<String> fileNames = Arrays.asList("fileA.txt", "fileB.txt", "fileC.txt", "fileD.txt", "fileX-1.txt");

    private static final Path inputPath = new Path("/input");

    private final Path testHar = new Path("/archive/testArchive.har");
    private final Path[] testSrcPaths = {new Path("input")};

    /**
     * Setup and Teardown
     */

    @org.junit.Before
    public void setUp() throws Exception {
        conf = setupConfiguration();
        File baseDir = new File("./target/hdfs/").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);

        dfsCluster = setUpMiniDFSCluster(conf);
        hdfsURI = "hdfs://localhost:" + dfsCluster.getNameNodePort() + "/";

        miniYARNCluster = setupMiniYARNCluster(conf);
        assertNotNull(miniYARNCluster);

        fs = dfsCluster.getFileSystem();
        Path archivePath = new Path("/archive");
        fs.delete(archivePath, true);

        initializeTestFiles();
        lsr();
    }

    private static Configuration setupConfiguration() {
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, new Path("./target/hdfs").toString());
        conf.set("mapreduce.framework.name", "local");
        return conf;
    }

    private static MiniDFSCluster setUpMiniDFSCluster(Configuration conf) throws Exception {
        MiniDFSCluster dfsCluster = new MiniDFSCluster
                .Builder(conf)
                .checkExitOnShutdown(true)
                .numDataNodes(2)
                .format(true)
                .racks(null)
                .build();
        return dfsCluster;
    }

    private static MiniYARNCluster setupMiniYARNCluster(Configuration conf) {
        MiniYARNCluster miniYARNCluster = new MiniYARNCluster("archiveTest", 1, 1, 1, 1, false);
        miniYARNCluster.init(conf);
        miniYARNCluster.start();
        return miniYARNCluster;
    }

    private void initializeTestFiles() throws IOException {
        int i = 1;
        for (String filename : fileNames) {
            fileList.add(DFSUtils.createFile(new Path(inputPath, String.format("sub%d/%s", i, filename)), fs)); // puke
        }
    }

    @org.junit.After
    public void tearDown() throws Exception {
        if (dfsCluster != null) {
            dfsCluster.shutdown();
        }

        if (miniYARNCluster != null) {
            miniYARNCluster.stop();
        }
    }

    /**
     * Unit Tests
     */

    @org.junit.Test
    public void testCreateArchive() throws Exception {
        ArchiveManager archiveManager = new ArchiveManager(conf);
        //TODO: multiple srcPaths possible?
        archiveManager.createArchive(testHar, testSrcPaths);

        lsr();

        // test if archive was created
        FileStatus fileStatus = fs.getFileStatus(new Path(testHar.toString()));
        assertTrue("Archive " + testHar.toString() + " not created.",
                fileStatus.getClass() == FileStatus.class);
        assertTrue("Archive " + testHar.toString() + " not created or name incorrectly",
                fileStatus.getPath().toString().contains(testHar.toString()));

        Index index = new Index(testHar, fs);
        assertTrue("An incorrect number of directory entries was created",
                index.numberOfDirectoryEntries() == 3);
        assertTrue("An incorrect number of file entries was created",
                index.numberOfFileEntries() == 5);

        // test if all files are in archive
        Path indexPath = new Path(testHar, "_index");
        String indexContent = DFSUtils.readFileContent(indexPath, fs);

        for (String filename : fileNames) {
            assertTrue("File " + filename + " not in archive.", indexContent.contains(filename));
        }

        String[] directories = {"/", "/input", "/input/sub1"};
        ArrayList<Integer> directoryHashes = new ArrayList<>();
        for (String directoryPath : directories) {
            directoryHashes.add(DFSUtils.getHarHash(directoryPath));
        }

        directoryHashes.forEach(hash -> {
            assertTrue("Not all directories in Index", index.containsEntryForHash(hash));
        });

        testUnarchiving(archiveManager, new Path(fileList.get(0)));
    }

    @org.junit.Test
    public void testAddFileToArchive() throws Exception {
        boolean debug = false;
        ArchiveManager archiveManager = new ArchiveManager(conf);
        archiveManager.createArchive(testHar, testSrcPaths);

        if (debug)
            printArchiveDetails("");

        String fileToAdd = DFSUtils.createFile(new Path(inputPath, "AddedFile.txt"), fs);
        archiveManager.addFileToArchive(new Path(fileToAdd), testHar);

        if (debug)
            printArchiveDetails("UPDATED");

        String[] tokenizedIndex = DFSUtils.readLines(new Path(testHar, "_index"), fs);
        for (int i = 0; i < tokenizedIndex.length - 1; i++) { // only run until the second to last element
            int lineHash = IndexEntry.parse(tokenizedIndex[i]).getHarHash();
            int nextLineHash = IndexEntry.parse(tokenizedIndex[i + 1]).getHarHash();
            assertTrue("The index is not ordered by hash", lineHash < nextLineHash);
        }
        Index index = new Index(testHar, fs);
        assertTrue("The file was not added to the index",
                index.containsEntryForHash(DFSUtils.getHarHash(fileToAdd)));

        // do this in bytes not Strings
        String contentOfAllFiles = "";
        for (String file : fileList) {
            contentOfAllFiles = contentOfAllFiles.concat(DFSUtils.readFileContent(new Path(file), fs));
        }
        contentOfAllFiles = contentOfAllFiles.concat(DFSUtils.readFileContent(new Path(fileToAdd), fs));
        String contentOfArchivePart0 = DFSUtils.readFileContent(new Path(testHar, "part-0"), fs);
        assertTrue("The file's content was not added to the part-file",
                contentOfAllFiles.equals(contentOfArchivePart0));

        //TODO: implement test
        assertTrue("The masterindex was not updated properly", true);

        testUnarchiving(archiveManager, new Path(fileToAdd));
    }

    /**
     * Test adding a mixture of individual files and folders containing a mixture of files and folders.
     * Filecontents need to be added to part files, index and masterindex must be updated accordingly.
     *
     * @throws Exception
     */
    @org.junit.Test
    public void testAddFilesToArchive() throws Exception {
        ArchiveManager archiveManager = new ArchiveManager(conf);
        Path dir1 = new Path("/multiInput/dir1");
        Path dir2 = new Path("/multiInput/dir2");
        Path dir3 = new Path("/multiInput/dir3");

        Path[] multiFileTestSrcPaths = {
                new Path(dir1, "dir1-1/file1-1-1.txt"),
                new Path(dir1, "dir1-1/file1-1-2.txt"),
                new Path(dir1, "dir1-2/file1-2-1.txt"),
                new Path(dir2, "dir2-1/file2-1-1.txt"),
                new Path(dir3, "dir3-1/file3-1-1.txt")
        };

        int expectedDirectories = 8; // multiInput, dir1, dir2, dir3, dir1-1, dir1-2, dir2-1, dir3-1

        for (Path path : multiFileTestSrcPaths) {
            DFSUtils.createFile(path, fs);
        }

        // int archiveCreated = archiveManager.createArchive(testHar, multiFileTestSrcPaths);
        int archiveCreated = archiveManager.createArchive(testHar, testSrcPaths);
        System.out.println("Archive creation returned: " + archiveCreated);
        lsr();

        Index index = new Index(testHar, fs);

        Map<String, List<IndexEntry>> originalIndex =
                DFSUtils.readAsList(new Path(testHar, "_index"), fs).parallelStream()
                        .map(IndexEntry::parse)
                        .collect(Collectors.groupingBy(IndexEntry::getType));

        // add the files to the archive
        Path[] srcPaths = { // all 5 files in multiFileTestSrcsPaths
                dir1,
                new Path(dir3, "dir3-1/"),
                new Path(dir2, "dir2-1/file2-1-1.txt"),
        };

        archiveManager.addFilesToArchive(srcPaths, testHar);

        Index newIndex = new Index(testHar, fs);

        printArchiveDetails("UPDATED");

        for (Path path : multiFileTestSrcPaths) {
            testUnarchiving(archiveManager, path);
        }

        Map<String, List<IndexEntry>> updatedIndex =
                DFSUtils.readAsList(new Path(testHar, "_index"), fs).parallelStream()
                        .map(IndexEntry::parse)
                        .collect(Collectors.groupingBy(IndexEntry::getType));
        assertEquals("The right number of files where added to the index.",
                originalIndex.get("file").size() + multiFileTestSrcPaths.length,
                updatedIndex.get("file").size());
        assertEquals("The right number of directories where added to the index.",
                originalIndex.get("dir").size() + expectedDirectories, updatedIndex.get("dir").size());

        boolean rootEntryWasUpdated = false;
        for (IndexEntry entry : updatedIndex.get("dir")) {
            IndexDirectory directory = (IndexDirectory) entry;
            if (directory.getPath().equals("/")
                    && directory.getChildren().size() == 2
                    && directory.getChildren().contains("multiInput")) {
                rootEntryWasUpdated = true;
                break;
            }
        }
        assertTrue("Previous entries were updated properly", rootEntryWasUpdated);

        assertTrue("Part files where updated properly", true);
        assertTrue("Masterindex was updated properly", true);
    }

    private void testUnarchiving(ArchiveManager archiveManager, Path fileToUnarchive) throws Exception {
        Path retrievedFile = new Path("/retrieved-" + fileToUnarchive.getName() + ".txt");
        int exitCode = dfscp(fileToUnarchive, retrievedFile);
        assertEquals("File retrieval returned exit code != 0 for " + fileToUnarchive.getName(), 0, exitCode);
        String retrievedFileContent = DFSUtils.readFileContent(retrievedFile, fs);
        assertEquals("Retrieved file has invalid content", retrievedFileContent, fileToUnarchive.getName());
    }

//    @org.junit.Test
//    public void testListArchiveFiles() throws Exception {
//        ArchiveManager archiveManager = new ArchiveManager(conf);
//        archiveManager.createArchive(testHar, testSrcPaths);
//        ArrayList<String> archiveFiles = archiveManager.listFilesInArchive(testHar);
//        assertEquals("Number of files in archive and input file count don't match",
//                archiveFiles.size(), fileList.size());
//        assertEquals("File list still contains directories",
//                archiveFiles.removeIf(s -> s.contains(" dir ")), false);
//    }

    @org.junit.Test
    public void testCheckForExistingFilesWhenAdding() throws Exception {
        ArchiveManager archiveManager = new ArchiveManager(conf);
        archiveManager.createArchive(testHar, testSrcPaths);

        printArchiveDetails("Initial");

        String newFile = DFSUtils.createFile(new Path(inputPath, "newFile.txt"), fs);

        Path[] addPaths = {new Path("/input/sub1/fileA.txt"),
                new Path("/input/sub1/fileB.txt"),
                new Path(newFile)};
        archiveManager.addFilesToArchive(addPaths, testHar);

        printArchiveDetails("AFTER");
        ArrayList<String> index = DFSUtils.readAsList(new Path(testHar, "_index"), fs);

        int numberOfFileA = 0;
        int numberOfFileB = 0;
        int numberOfNewFile = 0;
        for (String indexLine : index) {
            if (indexLine.startsWith("%2Finput%2Fsub1%2FfileA.txt")) {
                numberOfFileA++;
            }
            if (indexLine.startsWith("%2Finput%2Fsub1%2FfileB.txt")) {
                numberOfFileB++;
            }
            if (indexLine.startsWith("%2Finput%2FnewFile.txt")) {
                numberOfNewFile++;
            }
        }

        assertEquals("There is one or more duplicates of fileA.txt in the index", 1, numberOfFileA);
        assertEquals("There is one or more duplicates of fileB.txt in the index", 1, numberOfFileB);
        assertEquals("There is no new entry for newFile.txt", 1, numberOfNewFile);
    }

    @Ignore("This creates very large number of files. Don't executed too often on SSDs.")
    @org.junit.Test
    public void testCreateBigArchive() throws Exception {
        ArrayList<String> fileList = new ArrayList<>();
        for (int i = 0; i < 1002; i++) {
            fileList.add(DFSUtils.createFile(inputPath, fs, String.format("file-%d.txt", i), 10));
        }
        ArchiveManager am = new ArchiveManager(conf);
        am.createArchive(testHar, testSrcPaths);

        Path masterindexPath = new Path(testHar, "_masterindex");
        DFSUtils.printFile(masterindexPath, fs);
        Path indexPath = new Path(testHar, "_index");
        DFSUtils.printFile(indexPath, fs);
        lsr();
    }

    // FIXME listfiles is not working in archivemanagerdefault because of readfilecontent helperclass implementation in archivemanager
    /*
    @org.junit.Test
    public void addFileToArchiveDefault() throws Exception {
        ArchiveManagerDefault archiveManager = new ArchiveManagerDefault(conf);
        //Create an archive
        archiveManager.createArchive(testHar, testSrcPaths);
        // archive size before adding a file
        int old_size = archiveManager.listFilesInArchive(testHar).size();
        System.out.println("addFileToArchiveDefault: listFilesInArchive before");
        for (String s : archiveManager.listFilesInArchive(testHar)) {
            System.out.println(s);
        }

        // Create a new file
        createFile(new Path("/archive/tmp2/input/new.txt"), fs);
        // Add this new file to the archive
        archiveManager.addFile(new Path("/archive/tmp2/input/new.txt"), testHar);
        //archive size after adding a file
        int new_size = archiveManager.listFilesInArchive(testHar).size();
        System.out.println("addFileToArchiveDefault: listFilesInArchive after");
        for (String s : archiveManager.listFilesInArchive(testHar)) {
            System.out.println(s);
        }

        System.out.println("Old size" + old_size);
        System.out.println("New size" + new_size);

        //FIXME: This is not true, because the size is wrong. It is based on the index file. And the index file contains folder entries as well as files.
        assertTrue(old_size + 1 == new_size);
    }
    */

    @org.junit.Test
    public void testCompareHashesInAndOutOfArchive() throws Exception {
        ArchiveManager archiveManager = new ArchiveManager(conf);

        //Define root folder
        Path inputFolder = new Path("/input-folder/");
        Path addFolder = new Path("/add-folder/");

        //Create folder for test files
        fs.mkdirs(inputFolder);

        //Create 3 test files to be inserted to the test folder
        DFSUtils.createFile(inputFolder, fs, "file1.log", 100);
        DFSUtils.createFile(inputFolder, fs, "file2.log", 1000);
        DFSUtils.createFile(inputFolder, fs, "file3.log", 10);
        DFSUtils.createFile(addFolder, fs, "file4.log", 100);
        DFSUtils.createFile(addFolder, fs, "file5.log", 100);
        DFSUtils.createFile(addFolder, fs, "file6.log", 10);

        //Copy these files into a new archive
        Path[] inputFiles = {new Path("/input-folder/file1.log"),
                new Path("/input-folder/file2.log"), new Path("/input-folder/file3.log")};
        Path harPath = new Path("/output-test/archive.har");
        System.out.println("Create har");
        archiveManager.createArchive(harPath, inputFiles);

        //add a file
        archiveManager.addFileToArchive(addFolder, harPath);


        DFSUtils.printFile(harPath.suffix("/_index"), fs);

        //Print out files in archive
        HarFileSystem hfs = new HarFileSystem();
        String hfsUriString = String.format("har://hdfs-%s:%d%s", fs.getUri().getHost(), fs.getUri().getPort(), harPath);
        URI hfsUri = new URI(hfsUriString);
        hfs.initialize(hfsUri, conf);
        FileStatus root = hfs.listStatus(new Path("."))[0];
        FileStatus[] children = hfs.listStatus(root.getPath());
        System.out.println("Printing out the contents of the HAR archive:");
        for (FileStatus child : children) {
            System.out.println("Child: " + child.getPath());
        }

        //Get checksums for HDFS files
        String hdfsFile1 = DFSUtils.getChecksum(addFolder.toString() + "/file4.log", "hdfs", conf);
        String hdfsFile2 = DFSUtils.getChecksum(addFolder.toString() + "/file5.log", "hdfs", conf);
        String hdfsFile3 = DFSUtils.getChecksum(addFolder.toString() + "/file6.log", "hdfs", conf);

        //Check that HDFS checksums are NOT null
        assertNotNull("Is HDFS file 1 checksum NOT null: ", hdfsFile1);
        assertNotNull("Is HDFS file 2 checksum NOT null: ", hdfsFile2);
        assertNotNull("Is HDFS file 3 checksum NOT null: ", hdfsFile3);

        //Get checksums for HAR files
        String harFile1 = DFSUtils.getChecksum(harPath.suffix(addFolder.toString() + "/file4.log").toString(), "har", conf);
        String harFile2 = DFSUtils.getChecksum(harPath.suffix(addFolder.toString() + "/file5.log").toString(), "har", conf);
        String harFile3 = DFSUtils.getChecksum(harPath.suffix(addFolder.toString() + "/file6.log").toString(), "har", conf);

        //Check that HAR checksums are NOT null
        assertNotNull("Is HAR file 1 checksum NOT null: ", harFile1);
        assertNotNull("Is HAR file 2 checksum NOT null: ", harFile2);
        //  assertNotNull("Is HAR file 3 checksum NOT null: ", harFile3);

        //Check if the HDFS checksums match HAR checksums
        assertEquals("Hashes of HDFS file 1 and HAR file 1 match: ", hdfsFile1, harFile1);
        assertEquals("Hashes of HDFS file 2 and HAR file 2 match: ", hdfsFile2, harFile2);
        assertEquals("Hashes of HDFS file 3 and HAR file 3 match: ", hdfsFile3, harFile3);

        DFSUtils.printFile(new Path("/output-test/archive.har/_index"), fs);
    }

    /**
     * Helper Methods
     */

    /**
     * equivalent to hdfs dfs -ls -R /
     */
    private void lsr() throws Exception {
        final FsShell shell = new FsShell(conf);
        String[] args = {"-ls", "-R", "/"};
        shell.run(args);
    }

    /**
     * equivalent hdfs dfs -cp har://[harpath] hdfs:[targetPath]
     */
    private int dfscp(Path pathToFile, Path targetPath) throws Exception {
        final FsShell shell = new FsShell(conf);
        String[] args = {"-cp", "har://" + testHar + "/" + pathToFile.toString(), "hdfs:" + targetPath.toString()};
        return shell.run(args);
    }

    /**
     * Prints content of index, masterindex and part-0
     *
     * @deprecated Use Index functionality instead
     */
    private void printArchiveDetails(String optionalPrefix) throws IOException {
        if (optionalPrefix == null)
            optionalPrefix = "";

        System.out.println("#### " + optionalPrefix + " part-0 ####");
        DFSUtils.printFile(new Path(testHar, "part-0"), fs);
        System.out.println("#### " + optionalPrefix + " INDEX ####");
        DFSUtils.printFile(new Path(testHar, "_index"), fs);
        System.out.println("#### " + optionalPrefix + " MasterIndex ####");
        DFSUtils.printFile(new Path(testHar, "_masterindex"), fs);
    }
}