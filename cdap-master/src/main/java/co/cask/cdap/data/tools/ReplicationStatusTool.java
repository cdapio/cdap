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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
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
  private static boolean debugDump = false;
  private static String delimiter = ":";

  public static void main(String args[]) throws Exception {

    parseArgs(args);
    if (debugDump) {
      dumpReplicationStateTable();
      return;
    }

    if ((onMasterCluster && outputStatusFileName == null)
      || (!onMasterCluster && inputStatusFileName == null)) {
      showHelp();
      return;
    } else if (onMasterCluster) {
      processMasterCluster();
    } else {
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
          case 'd':
            // Debug dump
            debugDump = true;
            i += 1;
            break;
        }
      } else {
        return;
      }
    }
  }

  private static void showHelp() {
    System.out.println("\nTool to check Replication Status.");
    System.out.println("Run this tool on the Master cluster:");
    System.out.println("#ReplicationStatusTool -m -o <output filepath>");
    System.out.println("Then run on the Slave Cluster:");
    System.out.println("#ReplicationStatusTool -i <file copied from Master>\n");
    return;
  }

  private static void processMasterCluster() throws IOException {
    //Get checksum from Snapshot files
    masterChecksum = getClusterChecksum();
    masterShutdownTime = getShutdownTime();

    if (masterShutdownTime == 0L) {
      System.out.println("CDAP Shutdown time not available. Please run after CDAP has been shut down.");
      return;
    }

    //Scan the table and write to output file
    writeMasterStatusFile(getMapFromTable(ReplicationConstants.ReplicationStatusTool.WRITE_TIME_ROW_TYPE),
                          outputStatusFileName);
    System.out.println("Copy the file " + outputStatusFileName + " to the Slave Cluster and run tool there.");
  }

  private static void processSlaveCluster() throws Exception {
    // Read Master File for checksum, shutdown time and  writeTime for all regions from the Master Cluster
    Map<String, Long> masterTimeMap = readMasterStatusFile(inputStatusFileName);

    if (masterShutdownTime == 0L) {
      System.out.println("Could not read CDAP Shutdown Time from input file."
                           + " Please make sure CDAP has been shutdown on the Master and rerun the tool on Master.");
      return;
    }
    if (masterChecksum == null) {
      System.out.println("Could not read Master Checksum from input file.");
    }
    if (masterTimeMap == null || masterTimeMap.size() == 0) {
      System.out.println("No region information found in the input file");
    }

    checkHBaseReplicationComplete(masterTimeMap);
    //checkSnapshotReplicationComplete(masterChecksum);
  }

  private static void checkHBaseReplicationComplete(Map<String, Long> masterTimeMap) throws Exception {
    boolean complete = true;

    // Get replicate Time from table for all regions
    Map<String, Long> slaveTimeMap =
      getMapFromTable(ReplicationConstants.ReplicationStatusTool.REPLICATE_TIME_ROW_TYPE);

    //Check if all regions are accounted on both clusters
    if (masterTimeMap.size() != slaveTimeMap.size()) {
      System.out.println("Number of regions on the Master and Slave Clusters do not match.");
      complete = false;
    }

    complete = !checkDifferences(masterTimeMap.keySet(), slaveTimeMap.keySet());

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

  static boolean checkDifferences(Set<String> masterRegions, Set<String> slaveRegions) {
    boolean found = false;
    Set<String> extraInMaster = Sets.difference(masterRegions, slaveRegions);
    Set<String> extraInSlave = Sets.difference(slaveRegions, masterRegions);

    for (String regionName : extraInMaster) {
      System.out.println("Region=" + regionName + " found on Master but not on Slave Cluster.");
      found = true;
    }

    for (String regionName : extraInSlave) {
      System.out.println("Region=" + regionName + " found on Slave but not on Master Cluster.");
      found = true;
    }
    return found;
  }

  private static Map<String, Long> readMasterStatusFile(String masterStatusFile) throws IOException {
    Map<String, Long> map = new HashMap<String, Long>();
    try (BufferedReader br = new BufferedReader(new FileReader(masterStatusFile))) {
      String line;
      //Read the shutdown Time
      masterShutdownTime = new Long(br.readLine());
      //Read the checksum
      masterChecksum = br.readLine();

      //Read the rest into the region map
      while ((line = br.readLine()) != null) {
        StringTokenizer st = new StringTokenizer(line, delimiter);
        String region = st.nextToken();
        Long timestamp = new Long(st.nextToken());
        map.put(region, timestamp);
      }
    }
    return map;
  }

  private static void writeMasterStatusFile(Map<String, Long> map, String filePath) throws IOException {
    try (BufferedWriter bw =  new BufferedWriter(new FileWriter(filePath))) {
      //Write shutdown time
      bw.write(masterShutdownTime.toString());
      bw.newLine();
      //Write checksum
      bw.write(masterChecksum);
      bw.newLine();

      //Write region map
      for (Map.Entry<String, Long> entry : map.entrySet()) {
        bw.write(entry.getKey() + delimiter + entry.getValue());
        bw.newLine();
      }
    }
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
    HBaseTableUtil tableUtil = new HBaseTableUtilFactory(cConf).get();
    HTable hTable = tableUtil.createHTable(hConf, getReplicationStateTableId(tableUtil));
    ScanBuilder scan = tableUtil.buildScan();
    scan.addColumn(Bytes.toBytes(ReplicationConstants.ReplicationStatusTool.TIME_FAMILY),
                   Bytes.toBytes(ReplicationConstants.ReplicationStatusTool.WRITE_TIME_ROW_TYPE));
    scan.addColumn(Bytes.toBytes(ReplicationConstants.ReplicationStatusTool.TIME_FAMILY),
                   Bytes.toBytes(ReplicationConstants.ReplicationStatusTool.REPLICATE_TIME_ROW_TYPE));
    Result result;

    LOG.info("Dump Contents of the Table from Local Cluster.");
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

  private static void dumpMap(HashMap<String, Long> regionMap) {
    for (Map.Entry<String, Long> entry: regionMap.entrySet()) {
      System.out.println(entry.getKey() + "=>" + " timestamp=" + entry.getValue());
    }
  }

  private static String getClusterChecksum() throws IOException {
    return "checksum";
  }
}
