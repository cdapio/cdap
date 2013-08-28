package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.common.conf.CConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * Common utilities for all HBase datasets.
 */
public class HBaseTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableUtil.class);

  public static final String DEFAULT_TABLE_PREFIX = "continuuity";
  public static final String CFG_TABLE_PREFIX = "data.table.prefix";

  public static String getTablePrefix(CConfiguration conf) {
    return conf.get(CFG_TABLE_PREFIX, DEFAULT_TABLE_PREFIX);
  }

  public static String getHBaseTableName(CConfiguration conf, String tableName) {
    return getHBaseTableName(getTablePrefix(conf), tableName);
  }

  private static String encodeTableName(String tableName) {
    try {
      return URLEncoder.encode(tableName, "ASCII");
    } catch (UnsupportedEncodingException e) {
      // this can never happen - we know that ASCII is a supported character set!
      LOG.error("Error encoding table name '" + tableName + "'", e);
      throw new RuntimeException(e);
    }
  }

  public static String getHBaseTableName(String prefix, String tableName) {
    return prefix + "_" + encodeTableName(tableName);
  }
}
