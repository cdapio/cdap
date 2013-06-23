package com.continuuity.metrics2.temporaldb.internal;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metrics2.temporaldb.DataPoint;
import com.continuuity.metrics2.temporaldb.TemporalDataStore;
import com.continuuity.metrics2.temporaldb.KVStore;
import com.continuuity.metrics2.temporaldb.Query;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.*;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Concrete implementation of TemporalDataStore using LevelDB.
 * http://code.google.com/p/leveldb/
 */
public class LevelDBTemporalDataStore implements TemporalDataStore, KVStore {
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

  public LevelDBTemporalDataStore(File levelDBStore) {
    this.levelDBStore = levelDBStore;
    this.metrics = new UniqueId(this, "metrics");
    this.tags = new UniqueId(this, "tags");
    this.tagvalues = new UniqueId(this, "tagvalues");
    this.serializer = new DataPointSerializer(metrics, tags, tagvalues);
  }

  /**
   * Opens a database.
   * @throws Exception
   */
  @Override
  public void open(CConfiguration configuration) throws Exception {
    if (database == null) {
      initialize(configuration);
    }
  }

  /**
   * Closes the leveldb datastore.
   * @throws Exception
   */
  @Override
  public void close() {
    try {
      if (database != null) {
        database.close();
        database = null;
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } finally {
      database = null;
    }
  }

  /**
   * @return Gets a instance of levelDB database.
   * @throws Exception throws a checked exception during setup.
   */
  private DB getDatabase() throws Exception {
    if (database == null) {
      throw new IllegalStateException();
    }
    return database;
  }

  /**
   * Initializes the levelDB
   * @throws Exception
   */
  private void initialize(CConfiguration configuration) throws Exception {
    Options options = new Options();
    options.createIfMissing(true);
    options.compressionType(CompressionType.SNAPPY);
    if (dbFactory == null) {
      dbFactory = new JniDBFactory();
    }
    database = dbFactory.open(levelDBStore, options);
  }

  /**
   * Put a datapoint into datastore.
   *
   * @param dataPoint to be stored into store.
   * @throws Exception
   */
  @Override
  public void put(DataPoint dataPoint) throws Exception {
    try {
      put(null, dataPoint);
    } catch (Exception e) {
      throw new Exception(e);
    }
  }

  /**
   * Put a datapoint using the passed transaction Id.
   * NOTE: We have ignore txn for now to improve performance.
   *
   * @param txn transaction instance.
   * @param dataPoint to be stored into store.
   * @throws Exception
   */
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

  /**
   * Given a <code>query</code> returns the timeseries associated with metric.
   *
   * @param query to be executed to retrieve the datapoints.
   * @return Immutable list of datapoints.
   * @throws Exception
   */
  @Override
  public ImmutableList<DataPoint> execute(Query query) throws Exception {
    int metricID = metrics.getOrCreateId(query.getMetricName());
    int[][] tagFilter = serializer.convertTags(query.getTagFilter());
    byte[] startKey = serializer.createkey(metricID, query.getStartTime(),
                                           null);

    final KeyBasedQueryFilter keyBasedQueryFilter = new KeyBasedQueryFilter(
      metricID, query.getStartTime(), query.getEndTime(), tagFilter);

    // Get handle to DB and set the start key as iterator.
    DB database = getDatabase();
    DBIterator iterator = database.iterator();

    // Set the start key
    iterator.seek(startKey);

    Map<Long, DataPoint> points = Maps.newTreeMap();

    // Iterate through all the points.
    while (iterator.hasNext()) {
      Entry<byte[], byte[]> entry = iterator.next();
      byte[] key = entry.getKey();

      // Get timestamp, metric id, tags.
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

          // If there is a point at a given timestamp, then the
          // metric value are added.
          if(points.containsKey(parsedTimestamp)) {
            double value = points.get(parsedTimestamp).getValue()
                    + dp.getValue();
            points.put(parsedTimestamp,
              new DataPoint.Builder(dp.getMetric()).addTimestamp(parsedTimestamp)
                  .addValue(value).addTags(dp.getTags()).create());
          } else {
            points.put(parsedTimestamp,
              new DataPoint.Builder(dp.getMetric()).addTimestamp(parsedTimestamp)
                .addValue(dp.getValue()).addTags(dp.getTags()).create());
          }
        }
      } else {
        break;
      }
    }
    return ImmutableList.copyOf(points.values());
  }

  /**
   * Puts a key and value to store.
   *
   * @param key to store the value under.
   * @param value to be stored under key.
   * @throws Exception
   */
  @Override
  public void put(byte[] key, byte[] value) throws Exception {
    try {
      put(null, key, value);
    } catch (Exception e) {
      throw e;
    }
  }

  /** Implementation using the transaction */
  private void put(Object txn, byte[] key, byte[] value) throws Exception {
    try {
      DB database = getDatabase();
      database.put(key, value);
    } catch (Exception e) {
      throw e;
    }
  }

  /**
   * For a given <code>key</code> returns the byte array representation of value.
   *
   * @param key for which the value should be retrieved.
   * @return byte array of value.
   * @throws Exception
   */
  @Override
  public byte[] get(byte[] key) throws Exception {
    try {
      DB database = getDatabase();
      return database.get(key);
    } catch (Exception e) {
      throw e;
    }
  }
}