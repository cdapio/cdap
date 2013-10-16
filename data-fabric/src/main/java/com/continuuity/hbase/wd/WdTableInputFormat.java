package com.continuuity.hbase.wd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * Convert HBase tabular data into a format that is consumable by Map/Reduce with respect to
 * row key distribution strategy
 */
public class WdTableInputFormat extends TableInputFormat {

  public static final String ROW_KEY_DISTRIBUTOR_CLASS = "hbase.mapreduce.scan.wd.distributor.class";
  public static final String ROW_KEY_DISTRIBUTOR_PARAMS = "hbase.mapreduce.scan.wd.distributor.params";

  private AbstractRowKeyDistributor rowKeyDistributor;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);

    if (conf.get(ROW_KEY_DISTRIBUTOR_CLASS) != null) {
      String clazz = conf.get(ROW_KEY_DISTRIBUTOR_CLASS);
      try {
        rowKeyDistributor = (AbstractRowKeyDistributor) Class.forName(clazz).newInstance();
        if (conf.get(ROW_KEY_DISTRIBUTOR_PARAMS) != null) {
          rowKeyDistributor.init(conf.get(ROW_KEY_DISTRIBUTOR_PARAMS));
        }
      } catch (Exception e) {
        throw new RuntimeException("Cannot create row key distributor, " + ROW_KEY_DISTRIBUTOR_CLASS + ": " + clazz, e);
      }
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    List<InputSplit> allSplits = new ArrayList<InputSplit>();
    Scan originalScan = getScan();

    Scan[] scans = rowKeyDistributor.getDistributedScans(originalScan);

    for (Scan scan : scans) {
      // Internally super.getSplits(...) uses scan object stored in private variable,
      // to re-use the code of super class we switch scan object with scans we
      setScan(scan);
      List<InputSplit> splits = super.getSplits(context);
      allSplits.addAll(splits);
    }

    // Setting original scan back
    setScan(originalScan);

    return allSplits;
  }
}
