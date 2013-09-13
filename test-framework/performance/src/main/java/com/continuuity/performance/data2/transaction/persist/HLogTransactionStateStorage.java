package com.continuuity.performance.data2.transaction.persist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class HLogTransactionStateStorage implements TransactionStateStorage {

  private static final String HLOG_NAME = "TestHLogTransactionStore";
  private static final String OLD_LOG_DIR = ".old";

  private static final byte[] KV_FAMILY = Bytes.toBytes("f");
  private static final byte[] KV_QUALIFIER = Bytes.toBytes("q");

  private FileSystem fs;
  private Configuration conf;
  private Path rootDir;
  private HLog internalHLog;

  private static byte[] DUMMY_TABLE_NAME = Bytes.toBytes("t");
  private static HTableDescriptor HTD = new HTableDescriptor(DUMMY_TABLE_NAME);
  private static HRegionInfo DUMMY_REGION = new HRegionInfo(DUMMY_TABLE_NAME);


  public HLogTransactionStateStorage(Configuration conf, Path root) {
    this.conf = conf;
    this.rootDir = root;
  }

  public void init() throws IOException {
    fs = FileSystem.get(conf);
    internalHLog = new HLog(fs, new Path(rootDir, HLOG_NAME), new Path(rootDir, OLD_LOG_DIR), conf);
  }

  @Override
  public void append(TransactionEdit edit) throws IOException {
    long now = System.currentTimeMillis();
    WALEdit walEdit = createWALEdit(edit, now);
    internalHLog.append(DUMMY_REGION, DUMMY_TABLE_NAME, walEdit, now, HTD);
  }

  @Override
  public void append(List<TransactionEdit> edits) throws IOException {
    long now = System.currentTimeMillis();
    WALEdit walEdit = createWALEdit(edits, now);
    internalHLog.append(DUMMY_REGION, DUMMY_TABLE_NAME, walEdit, now, HTD);
  }

  private WALEdit createWALEdit(TransactionEdit edit, long now) {
    WALEdit walEdit = new WALEdit();
    walEdit.add(transactionToKeyValue(edit, now));
    return walEdit;
  }

  private WALEdit createWALEdit(List<TransactionEdit> edits, long now) {
    WALEdit walEdit = new WALEdit();
    for (TransactionEdit edit : edits) {
      walEdit.add(transactionToKeyValue(edit, now));
    }
    return walEdit;
  }

  private KeyValue transactionToKeyValue(TransactionEdit edit, long now) {
    return new KeyValue(Bytes.toBytes(edit.getTxId()), KV_FAMILY, KV_QUALIFIER, now, edit.toBytes());
  }

  public void close() throws IOException {
    if (internalHLog != null) {
      internalHLog.close();
    }
    if (fs != null) {
      fs.close();
    }
  }
}
