# Introduction
AHAR (Appendable Hadoop Archive) is a tool for Hadoop Distributed File System (HDFS) that allows you to add new files to an already existing Hadoop Archive (HAR). AHAR is executable without any HDFS modification. For questions and feedback please contact [Johannes](https://github.com/joh-mue) or [Thomas](https://github.com/trenner).

# How to use
1. Download or clone the repository and run mvn clean package -DskipTests
2. Set the system variable $HADOOP_HOME to your hadoop path. AHAR will automatically detect your running HDFS instance. Alternative, you can set the NameNode address as an argument in step 3.
3. Now you can run the jar in the target folder with the arguments cp <Files/Folder Path> <harPath>. For example: “*java -jar AppendableHadoopArchive-1.0-allinone.jar -cp /user/marc/file.log /user/marc/an.har”. You can also download the jar from here.

# Implementation
AHAR appends the binary data of the new files to one of the part-n files of the HAR archive using a first fit algorithm. Afterwards, the index entries in the masterindex and index files are updated and rewritten to HDFS.

# Known issues
AHAR uses HDFS append method to append new data to an existing HAR part files. Thus, you must have a cluster with three or more DataNodes or set part file replication to one for testing AHAR on a small cluster [HDFS-4600](https://issues.apache.org/jira/browse/HDFS-4600) or [HDFS-8960](https://issues.apache.org/jira/browse/HDFS-8960).
