package com.continuuity.data.hbase;

import com.google.common.base.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@link HBaseTestBase} implementation supporting HBase 0.94.
 */
public class HBase94Test extends HBaseTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(HBase94Test.class);

  @Override
  public HRegion createHRegion(byte[] tableName, byte[] startKey,
                               byte[] stopKey, String callingMethod, Configuration conf,
                               byte[]... families)
      throws IOException {
    if (conf == null) {
      conf = new Configuration();
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (byte [] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    HRegionInfo info = new HRegionInfo(htd.getName(), startKey, stopKey, false);
    Path path = new Path(conf.get(HConstants.HBASE_DIR), callingMethod);
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        throw new IOException("Failed delete of " + path);
      }
    }
    return HRegion.createHRegion(info, path, conf, htd);
  }

  @Override
  public void forceRegionFlush(byte[] tableName) throws IOException {
    if (hbaseCluster != null) {
      for (JVMClusterUtil.RegionServerThread t : hbaseCluster.getRegionServerThreads()) {
        List<HRegion> serverRegions = t.getRegionServer().getOnlineRegions(tableName);
        int cnt = 0;
        for (HRegion region : serverRegions) {
          region.flushcache();
          cnt++;
        }
        LOG.info("RegionServer {}: Flushed {} regions for table {}", t.getRegionServer().getServerName().toString(),
                 cnt, Bytes.toStringBinary(tableName));
      }
    }
  }

  @Override
  public void forceRegionCompact(byte[] tableName, boolean majorCompact) throws IOException {
    if (hbaseCluster != null) {
      for (JVMClusterUtil.RegionServerThread t : hbaseCluster.getRegionServerThreads()) {
        List<HRegion> serverRegions = t.getRegionServer().getOnlineRegions(tableName);
        int cnt = 0;
        for (HRegion region : serverRegions) {
          region.compactStores(majorCompact);
          cnt++;
        }
        LOG.info("RegionServer {}: Compacted {} regions for table {}", t.getRegionServer().getServerName().toString(),
                 cnt, Bytes.toStringBinary(tableName));
      }
    }
  }

  @Override
  public <T> Map<byte[], T> forEachRegion(byte[] tableName, Function<HRegion, T> function) {
    Map<byte[], T> results = new TreeMap<byte[], T>(Bytes.BYTES_COMPARATOR);
    // make sure consumer config cache is updated
    for (JVMClusterUtil.RegionServerThread t : hbaseCluster.getRegionServerThreads()) {
      List<HRegion> serverRegions = t.getRegionServer().getOnlineRegions(tableName);
      for (HRegion region : serverRegions) {
        results.put(region.getRegionName(), function.apply(region));
      }
    }
    return results;
  }
}
