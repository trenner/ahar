package de.tuberlin.cit.storageassistant;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by joh-mue on 06/03/17.
 */
public class AHarCLI {
    /**
     * -cp [hdfs://namenode:port] hdfs:/path/to/source hdfs:///path/to/archive.har
     *
     * @param args
     */
    public static void main(String[] args) {
        Configuration conf = initializeConfiguration();

        if ("-cp".equalsIgnoreCase(args[0])) {
            String command = new String(args[0]);
        } else {
            System.out.println("Usage: StorageAssitant.jar -cp [hdfs://namenode:port] hdfs:/path/to/source hdfs:///path/to/archive.har");
            System.exit(1);
        }

        Path[] srcPaths;
        String defaultFS = "";

        if (args[1].matches("hdfs:\\/\\/.+:\\d{4,6}")) {
            defaultFS = args[1];
            conf.set("fs.defaultFS", defaultFS);
            srcPaths = parseSrcPaths((String[]) ArrayUtils.subarray(args, 2, args.length - 1));
        } else {
            srcPaths = parseSrcPaths((String[]) ArrayUtils.subarray(args, 1, args.length - 1));


            Path harPath = new Path(args[args.length - 1]);

            if (everythingIsSet(srcPaths, harPath, defaultFS)) {
                try {
                    ArchiveManager archiveManager = new ArchiveManager(conf);
                    archiveManager.addFilesToArchive(srcPaths, harPath);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            } else {
                System.out.println("Usage: StorageAssitant.jar -cp [hdfs://namenode:port] hdfs:/path/to/source hdfs:///path/to/archive.har");
            }
        }
    }

    private static Configuration initializeConfiguration() {
        Configuration conf = new Configuration();

        String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome != null) {
            conf.addResource(new Path(hadoopHome + "etc/hadoop/core-site.xml"));
            conf.addResource(new Path(hadoopHome + "etc/hadoop/hdfs-site.xml"));
            conf.addResource(new Path(hadoopHome + "etc/hadoop/mapred-site.xml"));
        }
        return conf;
    }

    private static Path[] parseSrcPaths(String[] args) {
        Path[] srcPaths = new Path[args.length];
        for (int i = 0; i <= args.length - 1; i++) {
            srcPaths[i] = new Path(args[i]);
        }
        return srcPaths;
    }

    private static boolean everythingIsSet(Path[] srcPaths, Path harPath, String defaultFS) {
        if (System.getenv("HADOOP_HOME") == null && defaultFS.isEmpty()) {
            System.out.println("Please set $HADOOP_HOME or state a NameNode IP and port");
            System.exit(1);
        }
        return true;
    }
}
