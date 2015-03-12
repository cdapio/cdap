/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.leveldb;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBNameConverter;
import co.cask.cdap.data2.util.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

/**
 * Service maintaining all LevelDB tables.
 */
@Singleton
public class LevelDBTableService {

  private static final Logger LOG = LoggerFactory.getLogger(LevelDBTableService.class);

  private int blockSize;
  private long cacheSize;
  private String basePath;
  private WriteOptions writeOptions;

  private final ConcurrentMap<String, DB> tables = Maps.newConcurrentMap();

  private static final LevelDBTableService SINGLETON = new LevelDBTableService();

  public static LevelDBTableService getInstance() {
    return SINGLETON;
  }

  /**
   * Protect the constructor as this class needs to be singleton, but keep it package visible for testing.
   */
  @VisibleForTesting
  public LevelDBTableService() {
  }

  /**
   * For guice injecting configuration object to this singleton.
   */
  @Inject
  public void setConfiguration(CConfiguration config) throws IOException {
    basePath = config.get(Constants.CFG_DATA_LEVELDB_DIR);
    Preconditions.checkNotNull(basePath, "No base directory configured for LevelDB.");

    blockSize = config.getInt(Constants.CFG_DATA_LEVELDB_BLOCKSIZE, Constants.DEFAULT_DATA_LEVELDB_BLOCKSIZE);
    cacheSize = config.getLong(Constants.CFG_DATA_LEVELDB_CACHESIZE, Constants.DEFAULT_DATA_LEVELDB_CACHESIZE);
    writeOptions = new WriteOptions().sync(
      config.getBoolean(Constants.CFG_DATA_LEVELDB_FSYNC, Constants.DEFAULT_DATA_LEVELDB_FSYNC));
  }

  /**
   * only use in unit test since the singleton may be reused for multiple tests.
   */
  public void clearTables() throws IOException {
    for (String name : ImmutableList.copyOf(tables.keySet())) {
      dropTable(name);
    }
  }

  public Collection<String> list() throws Exception {
    File baseDir = new File(basePath);
    String[] subDirs = baseDir.list();
    if (subDirs == null) {
      return ImmutableList.of();
    }

    ImmutableCollection.Builder<String> builder = ImmutableList.builder();
    for (String dir : subDirs) {
      builder.add(getTableName(dir));
    }
    return builder.build();
  }

  /**
   * Gets tables stats.
   * @return map of table name -> table stats entries
   * @throws Exception
   */
  public Map<TableId, TableStats> getTableStats() throws Exception {
    File baseDir = new File(basePath);
    File[] subDirs = baseDir.listFiles();
    if (subDirs == null) {
      return ImmutableMap.of();
    }

    ImmutableMap.Builder<TableId, TableStats> builder = ImmutableMap.builder();
    for (File dir : subDirs) {
      String tableName = getTableName(dir.getName());
      // NOTE: we are using recursion to traverse file tree as we know that leveldb table fs tree is couple levels deep.
      long size = getSize(dir);
      builder.put(LevelDBNameConverter.from(tableName), new TableStats(size));
    }
    return builder.build();
  }

  // todo: use Guava's utils instead when we switch to v15+
  private static long getSize(File f) {
    if (f.isFile()) {
      return f.length();
    }

    File[] files = f.listFiles();
    if (files == null) {
      return 0;
    }

    long size = 0;
    for (File file : files) {
      size += getSize(file);
    }
    return size;
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

    // unfortunately, with the java version of leveldb, with createIfMissing set to false, factory.open will
    // see that there is no table and throw an exception, but it wont clean up after itself and will leave a
    // directory there with a lock.  So we want to avoid calling open if the path doesn't already exist and
    // throw the exception ourselves.
    File dbDir = new File(dbPath);
    if (!dbDir.exists()) {
      throw new IOException("Database " + dbPath + " does not exist and the create if missing option is disabled");
    }
    DB db = factory.open(dbDir, options);
    tables.put(tableName, db);
    return db;
  }

  private void createTable(String name) throws IOException {
    String dbPath = getDBPath(basePath, name);

    Options options = new Options();
    options.createIfMissing(true);
    options.errorIfExists(false);
    options.comparator(new KeyValueDBComparator());
    options.blockSize(blockSize);
    options.cacheSize(cacheSize);

    DB db = factory.open(new File(dbPath), options);
    tables.put(name, db);
  }

  public void dropTable(String name) throws IOException {
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
      encodedTableName = URLEncoder.encode(tableName, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.error("Error encoding table name '" + tableName + "'", e);
      throw new RuntimeException(e);
    }
    return new File(basePath, encodedTableName).getAbsolutePath();
  }

  private static String getTableName(String tableDir) throws UnsupportedEncodingException {
    return URLDecoder.decode(tableDir, "UTF-8");
  }

  /**
   * A comparator for the keys of key/value pairs.
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

  /**
   * Represents LevelDB's table stats.
   */
  public static final class TableStats {
    private final long diskSizeBytes;

    public TableStats(long sizeInBytes) {
      this.diskSizeBytes = sizeInBytes;
    }

    public long getDiskSizeBytes() {
      return diskSizeBytes;
    }
  }
}
