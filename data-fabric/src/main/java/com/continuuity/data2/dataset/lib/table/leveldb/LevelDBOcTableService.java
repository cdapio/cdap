package com.continuuity.data2.dataset.lib.table.leveldb;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.engine.leveldb.KeyValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.ConcurrentMap;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 * Service maintaining all LevelDB tables.
 */
@Singleton
public class LevelDBOcTableService {

  private static final Logger LOG = LoggerFactory.getLogger(LevelDBOcTableService.class);

  private int blockSize;
  private long cacheSize;
  private String basePath;
  private WriteOptions writeOptions;

  private final ConcurrentMap<String, DB> tables = Maps.newConcurrentMap();

  private static final LevelDBOcTableService SINGLETON = new LevelDBOcTableService();

  public static LevelDBOcTableService getInstance() {
    return SINGLETON;
  }

  /**
   * Protect the constructor as this class needs to be singleton.
   */
  private LevelDBOcTableService() {
  }

  /**
   * For guice injecting configuration object to this singleton.
   */
  @Inject
  public void setConfiguration(@Named("LevelDBConfiguration") CConfiguration config) throws IOException {
    basePath = config.get(Constants.CFG_DATA_LEVELDB_DIR);
    Preconditions.checkNotNull(basePath, "No base directory configured for LevelDB.");

    blockSize = config.getInt(Constants.CFG_DATA_LEVELDB_BLOCKSIZE, Constants.DEFAULT_DATA_LEVELDB_BLOCKSIZE);
    cacheSize = config.getLong(Constants.CFG_DATA_LEVELDB_CACHESIZE, Constants.DEFAULT_DATA_LEVELDB_CACHESIZE);
    writeOptions = new WriteOptions().sync(
      config.getBoolean(Constants.CFG_DATA_LEVELDB_FSYNC, Constants.DEFAULT_DATA_LEVELDB_FSYNC));
  }

  public WriteOptions getWriteOptions() {
    return writeOptions;
  }

  public DB getTable(String tableName) throws IOException {
    DB db = tables.get(tableName);
    if (db == null) {
      synchronized (tables) {
        db = tables.get(tableName);
        if (db == null) {
          db = openTable(tableName);
          tables.put(tableName, db);
        }
      }
    }
    return db;
  }

  public void ensureTableExists(String tableName) throws IOException {
    DB db = tables.get(tableName);
    if (db == null) {
      synchronized (tables) {
        db = tables.get(tableName);
        if (db == null) {
          createTable(tableName);
        }
      }
    }
  }

  private DB openTable(String tableName) throws IOException {
    String dbPath = getDBPath(basePath, tableName);

    Options options = new Options();
    options.createIfMissing(false);
    options.errorIfExists(false);
    options.comparator(new KeyValueDBComparator());
    options.blockSize(blockSize);
    options.cacheSize(cacheSize);

    return factory.open(new File(dbPath), options);
  }

  private void createTable(String name) throws IOException {
    String dbPath = getDBPath(basePath, name);

    Options options = new Options();
    options.createIfMissing(true);
    options.errorIfExists(false);
    options.comparator(new KeyValueDBComparator());
    options.blockSize(blockSize);
    options.cacheSize(cacheSize);

    DB db = null;
    try {
      db = factory.open(new File(dbPath), options);
    } finally {
      try {
        if (db != null) {
          db.close();
        }
      } catch (IOException e) {
        LOG.warn("Error closing LevelDB database", e);
        // but what else can we do? nothing?
      }
    }
  }

  public void dropTable(String name) throws Exception {
    DB db = tables.remove(name);
    if (db != null) {
      db.close();
    }
    String dbPath = getDBPath(basePath, name);
    factory.destroy(new File(dbPath), new Options());
  }


  private static String getDBPath(String basePath, String tableName) {
    String encodedTableName;
    try {
      encodedTableName = URLEncoder.encode(tableName, "ASCII");
    } catch (UnsupportedEncodingException e) {
      LOG.error("Error encoding table name '" + tableName + "'", e);
      throw new RuntimeException(e);
    }
    return new File(basePath, encodedTableName).getAbsolutePath();
  }

  /**
   * A comparator for the keys of HBase key/value pairs.
   */
  public static class KeyValueDBComparator implements DBComparator {
    @Override
    public int compare(byte[] left, byte[] right) {
      return KeyValue.KEY_COMPARATOR.compare(left, right);
    }
    @Override
    public byte[] findShortSuccessor(byte[] key) {
      return key;
    }
    @Override
    public byte[] findShortestSeparator(byte[] start, byte[] limit) {
      return start;
    }
    @Override
    public String name() {
      return "hbase-kv";
    }
  }
}
