package com.continuuity.metrics2.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.iq80.leveldb.*;

/**
 * Created with IntelliJ IDEA.
 * User: nmotgi
 * Date: 10/7/12
 * Time: 5:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class LevelDBDataStore implements DataStore, KVStore {
  /**
   * LevelDB Datastore.
   */
  private DB database;

  /**
   * LevelDB Store Directory.
   */
  private final File levelDBStore;

  /**
   * UniqueID for metrics.
   */
  private final UniqueId metrics;

  /**
   * UniqueID for tags.
   */
  private final UniqueId tags;

  /**
   * Unique ID for tag values.
   */
  private final UniqueId tagvalues;

  /**
   * LevelDB Factory.
   */
  private DBFactory dbFactory = null;

  /**
   * Datapoint serializer.
   */
  private final DataPointSerializer serializer;

  public LevelDBDataStore(String levelDBStore) {
    this(new File(levelDBStore));
  }

  public DBFactory getDbFactory() {
    return dbFactory;
  }

  public void setDbFactory(DBFactory dbFactory) {
    this.dbFactory = dbFactory;
  }

  public LevelDBDataStore(File levelDBStore) {
    this.levelDBStore = levelDBStore;
    this.metrics = new UniqueId(this, "metrics");
    this.tags = new UniqueId(this, "tags");
    this.tagvalues = new UniqueId(this, "tagvalues");
    this.serializer = new DataPointSerializer(metrics, tags, tagvalues);
  }

  public void close() throws Exception {
    try {
      if (database != null) {
        database.close();
        database = null;
      }
    } finally {
      database = null;
    }
  }

  private DB getDatabase() throws IOException {
    if (database == null) {
      initEnv();
    }
    if (database == null) {
      throw new IllegalStateException();
    }
    return database;
  }

  private void initEnv() throws IOException {
    Options options = new Options();
    options.createIfMissing(true);
    if (dbFactory == null) {
      throw new IllegalStateException("DBFactory is not set");
    }
    database = dbFactory.open(levelDBStore, options);
  }

  public void put(DataPoint dataPoint) throws IOException {
    try {
      DB database = getDatabase();
      put(null, dataPoint);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void put(Object txn, DataPoint dataPoint) throws Exception {
    String metricName = dataPoint.getMetric();
    if (metricName == null || metricName.isEmpty()) {
      throw new IllegalArgumentException(
        "Metric name should not be empty");
    }
    int metricID = metrics.getOrCreateId(metricName);
    long timestamp = dataPoint.getTimestamp();
    int[][] properties = serializer.convertTags(dataPoint.getTags());
    double value = dataPoint.getValue();
    byte[] key = serializer.createkey(metricID, timestamp, properties);
    byte[] valueBytes = Bytes.fromDouble(value);
    put(txn, key, valueBytes);
  }

  public void putMultiple(List<DataPoint> dataPoints) throws Exception {
    if (dataPoints == null || dataPoints.isEmpty()) {
      return;
    }
    // Transaction txn = null;
    try {
      DB database = getDatabase();
      WriteBatch batch = database.createWriteBatch();
      // batch.
      // txn = env.beginTransaction(null, null);
      for (DataPoint dataPoint : dataPoints) {
        Entry<byte[], byte[]> entry = serializer.convert(dataPoint);
        batch.put(entry.getKey(), entry.getValue());
      }
      database.write(batch);
      // txn.commit();
    } catch (Exception e) {
      // if (txn != null) {
      // txn.abort();
      // }
      throw e;
    }
  }

  public ImmutableList<DataPoint> getDataPoints(Query query) throws Exception {
    int metricID = metrics.getOrCreateId(query.getMetricName());
    int[][] tagFilter = serializer.convertTags(query.getTagFilter());
    byte[] startKey = serializer.createkey(metricID, query.getStartTime(),
                                           null);

    final KeyBasedQueryFilter keyBasedQueryFilter = new KeyBasedQueryFilter(
      metricID, query.getStartTime(), query.getEndTime(), tagFilter);

    // Get handle to DB and set the start key as iterator.
    DB database = getDatabase();
    DBIterator iterator = database.iterator();
    iterator.seek(startKey);
    final List<DataPoint> result = new ArrayList<DataPoint>();
    Function<DataPoint, Void> callback = query.getCallback();
    while (iterator.hasNext()) {
      Entry<byte[], byte[]> entry = iterator.next();
      byte[] key = entry.getKey();
      long parsedTimestamp = DataPointSerializer.parseTimeStamp(key);
      int parsedMetricID = DataPointSerializer.parseMetricID(key);
      int[][] parsedTags = DataPointSerializer.parseProperties(key);
      if (parsedMetricID == metricID
        && parsedTimestamp >= query.getStartTime()
        && parsedTimestamp < query.getEndTime()) {
        if (keyBasedQueryFilter.apply(parsedMetricID, parsedTimestamp,
                                      parsedTags)) {
          final DataPoint dp = serializer
            .convert(parsedMetricID, parsedTimestamp,
                     parsedTags, entry.getValue());
          if(callback != null) {
            callback.apply(dp);
          }
          result.add(dp);
        }
      } else {
        break;
      }
    }
    return ImmutableList.copyOf(result);
  }

  public void put(byte[] key, byte[] value) throws Exception {
    try {
      put(null, key, value);
    } catch (Exception e) {
      throw e;
    }
  }

  private void put(Object txn, byte[] key, byte[] value) throws Exception {
    try {
      DB database = getDatabase();
      database.put(key, value);
    } catch (Exception e) {
      throw e;
    }
  }

  public byte[] get(byte[] key) throws Exception {
    try {
      DB database = getDatabase();
      return database.get(key);
    } catch (Exception e) {
      throw e;
    }
  }

  public void open() throws Exception {
    if (database == null) {
      initEnv();
    }

  }

}