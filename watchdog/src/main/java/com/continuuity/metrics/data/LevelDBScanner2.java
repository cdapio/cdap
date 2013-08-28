package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.data.engine.leveldb.KeyValue;
import com.continuuity.data.engine.leveldb.LevelDBOVCTable;
import com.continuuity.data.engine.leveldb.LevelDBOVCTableHandle;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.OmidTransactionException;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.TransactionResult;
import com.continuuity.data.operation.executor.omid.Undo;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.guice.MetricsAnnotation;
import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 *
 */
public class LevelDBScanner2 {
  static int blocksize = 256 * 1024;
  static long cachesize = 1024 * 1024 * 100;

  public static void main(String[] args) throws Exception {

    JniDBFactory.pushMemoryPool(1024 * 1024 * 100);
    try {
      DB db = setup();

      /*int lines = 100000;
      System.out.println("writing " + lines + " rows of dummy data");
      WriteOptions wo = new WriteOptions();
      wo.snapshot(false);
      wo.sync(false);
      for (int i = 0; i < lines; i++) {
        byte[] row = Bytes.toBytes((long) i);
        byte[] family = Bytes.toBytes("f");
        byte[] qualifier = Bytes.toBytes((long) i * 31);
        byte[] val = Bytes.toBytes((long) i * 532);
        KeyValue kv = new KeyValue(row, family, qualifier, System.currentTimeMillis(), KeyValue.Type.Put, val);

        db.put(kv.getKey(), kv.getValue(), wo);
      }*/
      String stats = db.getProperty("leveldb.stats");
      System.out.println("leveldb.stats = " + stats);


      int i = 0;
      int rows;
      long start, dur;

      DBIterator iter = db.iterator();
      iter.seekToFirst();
      start = System.currentTimeMillis();
      iter.seekToLast();
      dur = System.currentTimeMillis() - start;
      System.out.println(dur + " ms to seek to last");
      start = System.currentTimeMillis();
      iter.seekToFirst();
      dur = System.currentTimeMillis() - start;
      System.out.println(dur + " ms to seek to first");

      while (true) {
        i++;
        rows = 0;
        iter = db.iterator();
        start = System.currentTimeMillis();
        iter.seekToFirst();
        while (iter.hasNext()) {
          rows++;
          iter.next();
        }
        iter.close();
        dur = System.currentTimeMillis() - start;
        System.out.println("scan #" + i + " took " + dur + " ms, scanned " + rows + " rows");
      }
    } finally {
      JniDBFactory.popMemoryPool();
    }

    /*String basePath = "data";
    Integer blockSize = 1024;
    Long cacheSize = 104857600L;*/
  }

  private static DB setup() throws IOException {
    Options options = new Options();
    options.createIfMissing(true);
    options.errorIfExists(false);
    options.comparator(new LevelDBOVCTable.KeyValueDBComparator());
    options.blockSize(blocksize);
    options.cacheSize(cachesize);
    options.paranoidChecks(false);
    CompressionType type = options.compressionType();
    return factory.open(new File("singlenode/data/ldb_REACTOR.MetricsTable.ts.1"), options);
  }
}
