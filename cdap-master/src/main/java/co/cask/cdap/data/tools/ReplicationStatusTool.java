/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


package co.cask.cdap.data.tools;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.data2.util.hbase.ScanBuilder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.replication.ReplicationConstants;
import co.cask.cdap.replication.ReplicationStatusKey;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

/**
 * The Replication Status tool is used to track the status of replication from Master to a Slave Cluster.
 * The tool reads writeTime and replicateTime for all tables and uses it to determine if all CDAP tables have been
 * completely replicated to the Slave cluster.
 * */
public class ReplicationStatusTool {

  private static CConfiguration cConf = CConfiguration.create();
  private static Configuration hConf = HBaseConfiguration.create();
  protected static final Logger LOG = LoggerFactory.getLogger(ReplicationStatusTool.class);

  private static boolean onMasterCluster = false;
  private static String masterChecksum = null;
  private static Long masterShutdownTime = 0L;
  private static String inputStatusFileName = null;
  private static String outputStatusFileName = null;
  private static String dirsFileName = null;
  private static boolean debugDump = false;

  private static final Gson GSON = new Gson();

  // Default paths to be pulled from configuration
  private static final String[] ALL_CHECKSUM_PATHS_DEFAULT = {
    TxConstants.Manager.CFG_TX_SNAPSHOT_DIR 
  };

  private static Set<String> allChecksumPaths;

  /**
   * Carries the current replication state of the cluster
   */
  private static class ReplicationState {
    private final Long shutdownTime;
    private final SortedMap<String, String> hdfsState;
    private final Map<String, Long> hBaseState;

    ReplicationState(Long shutdownTime, SortedMap<String, String> hdfsState, Map<String, Long> hBaseState) {
      this.shutdownTime = shutdownTime;
      this.hdfsState = hdfsState;
      this.hBaseState = hBaseState;
    }
  }

  public static void main(String args[]) throws Exception {

    parseArgs(args);
    if (debugDump) {
      setupChecksumDirs();
      dumpClusterChecksums();
      dumpReplicationStateTable();
      return;
    }

    if ((onMasterCluster && outputStatusFileName == null)
      || (!onMasterCluster && inputStatusFileName == null)) {
      showHelp();
      return;
    } else if (onMasterCluster) {
      setupChecksumDirs();
      processMasterCluster();
    } else {
      setupChecksumDirs();
      processSlaveCluster();
    }
  }


  private static void parseArgs(String args[]) {
    for (int i = 0; i < args.length;) {
      if (args[i].charAt(0) == '-') {
        switch (args[i].charAt(1)) {
          case 'm':
            onMasterCluster = true;
            i += 1;
            break;
          case 'i':
            inputStatusFileName = args[i + 1];
            i += 2;
            break;
          case 'o':
            outputStatusFileName = args[i + 1];
            i += 2;
            break;
          case 'f':
            dirsFileName = args[i + 1];
            i += 2;
            break;
          case 'd':
            // Debug dump
            debugDump = true;
            i += 1;
            break;
          default:
            return;
        }
      } else {
        return;
      }
    }
  }

  private static void showHelp() {
    System.out.println("\nTool to check Replication Status.");
    System.out.println("Run this tool on the Master cluster:");
    System.out.println("#ReplicationStatusTool -m -o <output filepath> [-f <file with hdfs paths>]");
    System.out.println("Then run on the Slave Cluster:");
    System.out.println("#ReplicationStatusTool -i <file copied from Master> [-f <file with hdfs paths>]\n");
    return;
  }

  private static void setupChecksumDirs() throws IOException {
    allChecksumPaths = new HashSet<>();
    if (dirsFileName == null) {
      for (String pathKey : ALL_CHECKSUM_PATHS_DEFAULT) {
        allChecksumPaths.add(cConf.get(pathKey));
      }
    } else {
      LOG.info("Reading hdfs paths from file " + dirsFileName);
      try (BufferedReader br = new BufferedReader(new FileReader(dirsFileName))) {
        String line;
        while ((line = br.readLine()) != null) {
          allChecksumPaths.add(line);
        }
      }
    }
  }

  private static void processMasterCluster() throws IOException {
    masterShutdownTime = getShutdownTime();
    if (masterShutdownTime == 0L) {
      System.out.println("CDAP Shutdown time not available. Please run after CDAP has been shut down.");
      return;
    }

    ReplicationState masterReplicationState =
      new ReplicationState(masterShutdownTime,
                           getClusterChecksumMap(),
                           getMapFromTable(ReplicationConstants.ReplicationStatusTool.WRITE_TIME_ROW_TYPE));

    // write replication state to output file
    try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputStatusFileName))) {
      GSON.toJson(masterReplicationState, bufferedWriter);
    }
    System.out.println("Copy the file " + outputStatusFileName + " to the Slave Cluster and run tool there.");
  }

  private static void processSlaveCluster() throws Exception {
    // Read Master File for Master shutdown time, checksum map and last write time for all regions
    ReplicationState masterReplicationState = null;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(inputStatusFileName))) {
      masterReplicationState = GSON.fromJson(bufferedReader, ReplicationState.class);
    }

    if (masterReplicationState == null || masterReplicationState.shutdownTime == 0L) {
      System.out.println("Could not read CDAP Shutdown Time from input file."
                           + " Please make sure CDAP has been shutdown on the Master and rerun the tool on Master.");
      return;
    }
    if (masterReplicationState.hdfsState.isEmpty()) {
      System.out.println("No HDFS File Information found in the input file.");
    }
    if (masterReplicationState.hBaseState.isEmpty()) {
      System.out.println("No region information found in the input file");
    }

    checkHDFSReplicationComplete(masterReplicationState.hdfsState);
    checkHBaseReplicationComplete(masterReplicationState.hBaseState);
  }

  private static void checkHBaseReplicationComplete(Map<String, Long> masterTimeMap) throws Exception {
    boolean complete;

    // Get replicate Time from table for all regions
    Map<String, Long> slaveTimeMap =
      getMapFromTable(ReplicationConstants.ReplicationStatusTool.REPLICATE_TIME_ROW_TYPE);

    //Check if all regions are accounted on both clusters
    if (masterTimeMap.size() != slaveTimeMap.size()) {
      System.out.println("Number of regions on the Master and Slave Clusters do not match.");
    }

    complete = !checkDifferences(masterTimeMap.keySet(), slaveTimeMap.keySet(), "Region");

    //Check the maps for all regions
    for (Map.Entry<String, Long> writeTimeEntry : masterTimeMap.entrySet()) {
      String region = writeTimeEntry.getKey();
      Long writeTime = writeTimeEntry.getValue();
      Long replicateTime = slaveTimeMap.get(region);

      if (replicateTime != null && !isReplicationComplete(masterShutdownTime, writeTime, replicateTime)) {
        System.out.println("Region:" + region + " behind by " + (writeTime - replicateTime) + "ms.");
        complete = false;
      }
    }

    if (complete) {
      // If replication is complete for all regions.
      System.out.println("HBase Replication is complete.");
    } else {
      System.out.println("HBase Replication is NOT complete.");
    }
  }

  static boolean checkDifferences(Set<String> masterMap, Set<String> slaveMap, String keyName) {
    boolean different = false;
    Set<String> extraInMaster = Sets.difference(masterMap, slaveMap);
    Set<String> extraInSlave = Sets.difference(slaveMap, masterMap);

    for (String key : extraInMaster) {
      System.out.println(keyName + " " + key + " found on Master but not on Slave Cluster.");
      different = true;
    }

    for (String key : extraInSlave) {
      System.out.println(keyName + " "  + key + " found on Slave but not on Master Cluster.");
      different = true;
    }
    return different;
  }

  private static Map<String, Long> getMapFromTable(String rowType) throws IOException {
    HBaseTableUtil tableUtil = new HBaseTableUtilFactory(cConf).get();
    HTable hTable = tableUtil.createHTable(hConf, getReplicationStateTableId(tableUtil));

    // Scan the table to scan for all regions.
    ScanBuilder scan = getScanBuilder(tableUtil, rowType);
    Result result;
    HashMap<String, Long> timeMap = new HashMap<>();

    try (ResultScanner resultScanner = hTable.getScanner(scan.build())) {
      while ((result = resultScanner.next()) != null) {
        ReplicationStatusKey key = new ReplicationStatusKey(result.getRow());
        String region = key.getRegionName();
        Long timestamp = getTimeFromResult(result, rowType);

        if (timeMap.get(region) == null || timestamp > timeMap.get(region)) {
          timeMap.put(region, timestamp);
        }
      }
    } catch (Exception e) {
      LOG.error("Error while reading table.", e);
      throw Throwables.propagate(e);
    } finally {
      hTable.close();
    }
    return timeMap;
  }

  private static ScanBuilder getScanBuilder(HBaseTableUtil tableUtil, String rowType) {
    ScanBuilder scan = tableUtil.buildScan();

    // FIX: get scan based on start row and stop row
    // ReplicationStatusKey startKey = new ReplicationStatusKey(Bytes.toBytes(prefix));
    // scan.setStartRow(startKey.getKey());
    // scan.setStopRow(Bytes.stopKeyForPrefix(startKey.getKey()));

    scan.addColumn(Bytes.toBytes(ReplicationConstants.ReplicationStatusTool.TIME_FAMILY), Bytes.toBytes(rowType));
    scan.setMaxVersions(1);
    return scan;
  }

  private static TableId getReplicationStateTableId(HBaseTableUtil tableUtil) throws IOException {
    String tableName = hConf.get(ReplicationConstants.ReplicationStatusTool.REPLICATION_STATE_TABLE_NAME);
    String ns = hConf.get(ReplicationConstants.ReplicationStatusTool.REPLICATION_STATE_TABLE_NAMESPACE);
    TableId tableId = tableUtil.createHTableId(
      (ns != null)
        ? new NamespaceId(ns)
        : new NamespaceId(ReplicationConstants.ReplicationStatusTool.REPLICATION_STATE_TABLE_DEFAULT_NAMESPACE),
      (tableName != null)
        ? tableName
        : ReplicationConstants.ReplicationStatusTool.REPLICATION_STATE_TABLE_DEFAULT_NAME);
    return tableId;
  }

  private static Long getTimeFromResult(Result result, String rowType) {
    Long timestamp = 0L;
    ByteBuffer timestampBuffer = result.getValueAsByteBuffer(
      Bytes.toBytes(ReplicationConstants.ReplicationStatusTool.TIME_FAMILY), Bytes.toBytes(rowType));
    if (timestampBuffer != null) {
      timestamp = timestampBuffer.getLong();
    }
    return timestamp;
  }

  private static Long getShutdownTime() throws IOException {
    Long shutdownTime = 0L;
    File shutdownTimeFile = new File(System.getProperty("java.io.tmpdir"),
                                     Constants.Replication.CDAP_SHUTDOWN_TIME_FILENAME);
    try (BufferedReader br = new BufferedReader(new FileReader(shutdownTimeFile))) {
      //Read the shutdown Time
      shutdownTime = new Long(br.readLine());
    } catch (FileNotFoundException e) {
      LOG.error("Cannot read shutdown time.");
    }
    LOG.info("Read CDAP master shutdown time: {}", shutdownTime);
    return shutdownTime;
  }

  /**
   * This function implements the logic to test if replication is complete for a given table
   * @param shutdownTime CDAP shutdown time
   * @param writeTime timestamp of the last WAL entry written for this table
   * @param replicateTime timestamp of the last WAL entry replicated for this table
   * @return
   */
  private static boolean isReplicationComplete(long shutdownTime, long writeTime, long replicateTime) {
    if (replicateTime > shutdownTime) {
      // Anything after CDAP shutdown time can be ignored owing to non CDAP tables.
      return true;
    } else if (replicateTime >= writeTime) {
      // = condition: All entries written on the Master cluster have been replicated to the slave cluster.
      // > condition: In the meanwhile that the status file was copied from Master to the slave Cluster,
      // non CDAP tables could have had more updates.
      return true;
    }
    return false;
  }

  private static void dumpReplicationStateTable() throws Exception {

    System.out.println("\nThis is all the HBase regions on the Cluster:");
    HBaseTableUtil tableUtil = new HBaseTableUtilFactory(cConf).get();
    HTable hTable = tableUtil.createHTable(hConf, getReplicationStateTableId(tableUtil));
    ScanBuilder scan = tableUtil.buildScan();
    scan.addColumn(Bytes.toBytes(ReplicationConstants.ReplicationStatusTool.TIME_FAMILY),
                   Bytes.toBytes(ReplicationConstants.ReplicationStatusTool.WRITE_TIME_ROW_TYPE));
    scan.addColumn(Bytes.toBytes(ReplicationConstants.ReplicationStatusTool.TIME_FAMILY),
                   Bytes.toBytes(ReplicationConstants.ReplicationStatusTool.REPLICATE_TIME_ROW_TYPE));
    Result result;
    try (ResultScanner resultScanner = hTable.getScanner(scan.build())) {
      while ((result = resultScanner.next()) != null) {
        ReplicationStatusKey key = new ReplicationStatusKey(result.getRow());
        String rowType = key.getRowType();
        String region = key.getRegionName();
        UUID rsID = key.getRsID();

        Long writeTime = getTimeFromResult(result, ReplicationConstants.ReplicationStatusTool.WRITE_TIME_ROW_TYPE);
        Long replicateTime = getTimeFromResult(result,
                                               ReplicationConstants.ReplicationStatusTool.REPLICATE_TIME_ROW_TYPE);
        System.out.println("Key=>rowType:" + rowType + ":region:" + region + ":RSID:" + rsID
                             + " writeTime:" + writeTime + ":replicateTime:" + replicateTime);
      }
    } finally {
      hTable.close();
    }
  }

  private static void checkHDFSReplicationComplete(SortedMap<String, String> masterChecksumMap) throws IOException {
    boolean complete;
    SortedMap<String, String> slaveChecksumMap = getClusterChecksumMap();

    //Check if all files are accounted on both clusters
    if (masterChecksumMap.size() != slaveChecksumMap.size()) {
      System.out.println("Number of HDFS files on the Master and Slave Clusters do not match.");
    }

    complete = !checkDifferences(masterChecksumMap.keySet(), slaveChecksumMap.keySet(), "File");

    for (Map.Entry<String, String> checksumEntry : masterChecksumMap.entrySet()) {
      String file = checksumEntry.getKey();
      String masterChecksum = checksumEntry.getValue();
      String slaveChecksum = slaveChecksumMap.get(file);

      if (slaveChecksum != null && !masterChecksum.equals(slaveChecksum)) {
        System.out.println("Master Checksum " + masterChecksum + " for File " + file + " does not match with"
                             + " Slave Checksum " + slaveChecksum);
        complete = false;
      }
    }

    if (complete) {
      // If checksums match for all files.
      System.out.println("Master and Slave Checksums match. HDFS Replication is complete.");
    } else {
      System.out.println("HDFS Replication is NOT Complete.");
    }
  }

  private static String normalizedFileName(String fileName) {
    //while matching the filenames, ignore the IP addresses of the clusters
    String normalizedName;
    String ipv4Pattern = "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])";
    String ipv6Pattern = "([0-9a-f]{1,4}:){7}([0-9a-f]){1,4}";
    normalizedName = fileName.replaceFirst(ipv4Pattern, "<hostname>");
    normalizedName = normalizedName.replaceFirst(ipv6Pattern, "<hostname>");
    return normalizedName;
  }

  private static SortedMap<String, String> getClusterChecksumMap() throws IOException {
    FileSystem fileSystem = FileSystem.get(hConf);
    List<String> fileList = addAllFiles(fileSystem);
    SortedMap<String, String> checksumMap = new TreeMap<String, String>();
    for (String file : fileList) {
      FileChecksum fileChecksum = fileSystem.getFileChecksum(new Path(file));
      checksumMap.put(normalizedFileName(file), fileChecksum.toString());
    }
    LOG.info("Added " + checksumMap.size() + " checksums for snapshot files.");
    return checksumMap;
  }

  private static List<String> addAllFiles(FileSystem fs) throws IOException {
    List<String> fileList = new ArrayList<String>();
    for (String dirPathName : allChecksumPaths) {
      Path dirPath = new Path(dirPathName);
      LOG.info("Getting all files under " + dirPath);
      addAllDirFiles(dirPath, fs, fileList);
    }
    return fileList;
  }

  private static void addAllDirFiles(Path filePath, FileSystem fs, List<String> fileList) throws IOException {
    FileStatus[] fileStatus = fs.listStatus(filePath);
    for (FileStatus fileStat : fileStatus) {
      if (fileStat.isDirectory()) {
        addAllDirFiles(fileStat.getPath(), fs, fileList);
      } else {
        fileList.add(fileStat.getPath().toString());
      }
    }
  }

  private static void dumpClusterChecksums() throws IOException {
    System.out.println("\nThis is all the File Checksums on the Cluster:");
    SortedMap<String, String> checksumMap = getClusterChecksumMap();
    for (Map.Entry<String, String> checksumEntry : checksumMap.entrySet()) {
      System.out.println("File:" + checksumEntry.getKey() + " Checksum:" + checksumEntry.getValue());
    }
  }
}
