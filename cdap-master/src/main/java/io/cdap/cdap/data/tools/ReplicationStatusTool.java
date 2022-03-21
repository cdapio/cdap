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


package io.cdap.cdap.data.tools;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.util.TableId;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtilFactory;
import io.cdap.cdap.data2.util.hbase.ScanBuilder;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.replication.ReplicationConstants;
import io.cdap.cdap.replication.ReplicationStatusKey;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
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

  private static final Options options = new Options();
  private static final String HELP_OPTION = "h";
  private static final String INPUT_OPTION = "i";
  private static final String OUTPUT_OPTION = "o";
  private static final String MASTER_OPTION = "m";
  private static final String DEBUG_OPTION = "d";
  private static final String SHUTDOWNTIME_OPTION = "s";
  private static final String FILE_OPTION = "f";

  private static Long masterShutdownTime = 0L;
  private static String inputStatusFileName;
  private static String outputStatusFileName;
  private static String dirsFileName;
  private static long shutDownTimeArgument;

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
    CommandLine cmd;
    try {
      // parse the command line arguments
      cmd = parseArgs(args);
    } catch (ParseException e) {
      LOG.error("Error when parsing command-line arguemnts", e);
      printUsage();
      return;
    }

    if (cmd.hasOption(DEBUG_OPTION)) {
      setupChecksumDirs();
      dumpClusterChecksums();
      dumpReplicationStateTable();
      return;
    }
    inputStatusFileName = cmd.getOptionValue(INPUT_OPTION);
    outputStatusFileName = cmd.getOptionValue(OUTPUT_OPTION);
    dirsFileName = cmd.getOptionValue(FILE_OPTION);
    if (cmd.hasOption(SHUTDOWNTIME_OPTION)) {
      shutDownTimeArgument = Long.parseLong(cmd.getOptionValue(SHUTDOWNTIME_OPTION));
      if (shutDownTimeArgument < 0L) {
        System.out.println("Invalid ShutDown time.");
        return;
      }
    }

    if (cmd.hasOption(HELP_OPTION) || !sanityCheckOptions(cmd)) {
      printUsage();
      return;
    }
    if (cmd.hasOption(MASTER_OPTION)) {
      setupChecksumDirs();
      processMasterCluster();
    } else {
      setupChecksumDirs();
      processSlaveCluster();
    }
  }

  private static boolean sanityCheckOptions(CommandLine cmd) {
    if (cmd.hasOption(MASTER_OPTION) && !cmd.hasOption(OUTPUT_OPTION)) {
      System.out.println("No File Path provided for creating Master Status with option -" + OUTPUT_OPTION);
      return false;
    }
    if (!cmd.hasOption(MASTER_OPTION) && !cmd.hasOption(INPUT_OPTION)) {
      System.out.println("No File Path provided for reading Master Status with option -" + INPUT_OPTION);
      return false;
    }
    return true;
  }

  private static void printUsage() {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(80);
    String usageHeader = "Options:";
    String usageFooter = "";
    String usageStr = "cdap run " + ReplicationStatusTool.class + " <options>";
    helpFormatter.printHelp(usageStr, usageHeader, options, usageFooter);
  }

  private static CommandLine parseArgs(String[] args) throws ParseException {
    options.addOption(MASTER_OPTION, false, "Use when running on Master Cluster");
    options.addOption(OUTPUT_OPTION, true, "FilePath to dump Master Cluster Status");
    options.addOption(INPUT_OPTION, true, "Status File copied from the Master Cluster");
    options.addOption(FILE_OPTION, true, "File with HDFS Paths");
    options.addOption(SHUTDOWNTIME_OPTION, true, "Override cdap-master Shutdown Time on Master Cluster [epoch time]");
    options.addOption(DEBUG_OPTION, false, "Dump Cluster Status for debugging");
    options.addOption(HELP_OPTION, false, "Show this Usage");
    CommandLineParser parser = new BasicParser();
    return parser.parse(options, args);
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
    masterShutdownTime = shutDownTimeArgument != 0L ? shutDownTimeArgument : getShutdownTime();
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

    // Verify that all regions on the Master cluster are present on the Slave cluster
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

  // Verify that all entries on the Master Cluster are present on the Slave Cluster
  static boolean checkDifferences(Set<String> masterMap, Set<String> slaveMap, String keyName) {
    boolean different = false;
    Set<String> extraInMaster = Sets.difference(masterMap, slaveMap);
    for (String key : extraInMaster) {
      System.out.println(keyName + " " + key + " found on Master but not on Slave Cluster.");
      different = true;
    }
    return different;
  }

  private static Map<String, Long> getMapFromTable(String rowType) throws IOException {
    HBaseTableUtil tableUtil = new HBaseTableUtilFactory(cConf).get();

    // Scan the table to scan for all regions.
    ScanBuilder scan = getScanBuilder(tableUtil, rowType);
    Result result;
    HashMap<String, Long> timeMap = new HashMap<>();

    try (Table table = tableUtil.createTable(hConf, getReplicationStateTableId(tableUtil));
         ResultScanner resultScanner = table.getScanner(scan.build())) {
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
    File shutdownTimeFile = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                     Constants.Replication.CDAP_SHUTDOWN_TIME_FILENAME).getAbsoluteFile();
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

    ScanBuilder scan = tableUtil.buildScan();
    scan.addColumn(Bytes.toBytes(ReplicationConstants.ReplicationStatusTool.TIME_FAMILY),
                   Bytes.toBytes(ReplicationConstants.ReplicationStatusTool.WRITE_TIME_ROW_TYPE));
    scan.addColumn(Bytes.toBytes(ReplicationConstants.ReplicationStatusTool.TIME_FAMILY),
                   Bytes.toBytes(ReplicationConstants.ReplicationStatusTool.REPLICATE_TIME_ROW_TYPE));

    try (Table table = tableUtil.createTable(hConf, getReplicationStateTableId(tableUtil));
         ResultScanner resultScanner = table.getScanner(scan.build())) {
      Result result;
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
    }
  }

  private static void checkHDFSReplicationComplete(SortedMap<String, String> masterChecksumMap) throws IOException {
    boolean complete;
    SortedMap<String, String> slaveChecksumMap = getClusterChecksumMap();

    // Verify that all files on Master are present on Slave. Ignore any extra files on Slave. This could
    // happen when old snapshot files are pruned by CDAP on the Master cluster.
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
