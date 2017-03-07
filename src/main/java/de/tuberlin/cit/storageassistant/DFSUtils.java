package de.tuberlin.cit.storageassistant;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;

public abstract class DFSUtils {

    /** Helper Methods **/

    /**
     * Print and return the content of a file.
     *
     * @param path
     * @param fs
     * @return
     * @throws IOException
     */

    public static void printFile(Path path, FileSystem fs) throws IOException {
        //TODO use logging for this
        String[] content = readLines(path, fs);
        for (String line : content) {
            System.out.println(line.replace("%2F", "/"));
        }
    }

    /**
     * Read bytes from the given position in the stream and return as String.
     *
     * @param path
     * @param fs
     * @return
     * @throws IOException
     * @parm byteRange bytes supposed to be read. if null entire file is read
     */
    public static String readFileContent(Path path, FileSystem fs) throws IOException {
        FSDataInputStream in = fs.open(path);
        byte[] b = new byte[in.available()];
        in.readFully(b);
        in.close();
        return new String(b);
    }

    /**
     * Read bytes from the given position in the stream and return result as String[] of the files lines.
     *
     * @param path
     * @param fs
     * @return
     * @throws IOException
     * @parm byteRange bytes supposed to be read. if null entire file is read
     */
    public static String[] readLines(Path path, FileSystem fs) throws IOException {
        return readFileContent(path, fs).split("\n");
    }

    /**
     * Convenience Method.
     *
     * @param path
     * @param fs
     * @return
     * @throws IOException
     */
    public static ArrayList<String> readAsList(Path path, FileSystem fs) throws IOException {
        return new ArrayList<>(Arrays.asList(readLines(path, fs))); // \(^_^)\
    }

    /**
     * Creates a file and fills it with its name.
     *
     * @param path
     * @param fs
     * @return Path of the file that was created
     * @throws IOException
     */
    static String createFile(Path path, FileSystem fs) throws IOException {
        try (FSDataOutputStream outStream = fs.create(path)) {
            outStream.writeBytes(path.getName());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return path.toString();
    }

    /**
     * Creates a file and makes it as large as required.
     *
     * @param root
     * @param fs
     * @param filename
     * @param size     in bytes
     * @return Path of the file that was created
     * @throws IOException
     */
    static String createFile(Path root, FileSystem fs, String filename, int size) throws IOException {
        final Path path = new Path(root, filename);
        final FSDataOutputStream outStream = fs.create(path);
        try {
            for (int i = 0; i < size / 10; i++) {
                outStream.writeBytes(filename);
            }
        } finally {
            outStream.close();
        }
        System.out.println("Path: " + path.toString() + "->" + HarFileSystem.getHarHash(path));
        return path.toString();
    }

    public static Integer getHarHash(FileStatus fileStatus) {
        String path = fileStatus.getPath().toString();
        return getHarHash(path);
    }

    public static Integer getHarHash(String path) {
        String cleanedPath = path.replaceFirst("hdfs:\\/\\/.+:\\d{4,6}", "");
        return HarFileSystem.getHarHash(new Path(cleanedPath));
    }

    /**
     * Method for retrieving a checksum from a file in the HDFS installation.
     *
     * @param pathToFile - path to file, from which the checksum is to be read
     * @return - int representing the checksum hash
     */

    public static String getChecksum(String pathToFile, String fsSchema, Configuration conf)
            throws IOException, URISyntaxException, NoSuchAlgorithmException, NullPointerException {
        FileSystem fs = FileSystem.get(conf);
        String md5Checksum;

        if (fsSchema.equalsIgnoreCase("hdfs")) {
            FSDataInputStream fis = fs.open(new Path(fs.getUri().toString() + pathToFile));

            md5Checksum = DigestUtils.md5Hex(fis);
            fis.close();

            return md5Checksum;

        } else if (fsSchema.equalsIgnoreCase("har")) {
            HarFileSystem hfs = new HarFileSystem();

            String pathToArchive = pathToFile.split(".har")[0] + ".har";

            String hfsUriString = String.format("har://" + fs.getScheme() + "-%s:%d%s", fs.getUri().getHost(), fs.getUri().getPort(), pathToArchive);
            URI hfsURI = new URI(hfsUriString);
            hfs.initialize(hfsURI, conf);

            Path file = new Path(pathToFile);
            FSDataInputStream fis = hfs.open(file);

            md5Checksum = DigestUtils.md5Hex(fis);
            fis.close();

            return md5Checksum;

        } else {
            System.out.println("getChecksum: fs schema needs to be hdfs or har");
            return "null";
        }
    }

}
