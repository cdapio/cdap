/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.metadata.MetaDataEntry;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.GetSplits;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.KeyRange;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.Scan;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.TableOperation;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.WriteOperationComparator;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.TransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.queueproxy.QueueCallable;
import com.continuuity.data.operation.executor.omid.queueproxy.QueueRunnable;
import com.continuuity.data.operation.executor.omid.queueproxy.QueueStateProxy;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.EnqueueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueFinalize;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;
import com.continuuity.data.operation.ttqueue.TTQueue;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.operation.ttqueue.admin.GetGroupID;
import com.continuuity.data.operation.ttqueue.admin.GetQueueInfo;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigure;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigureGroups;
import com.continuuity.data.operation.ttqueue.admin.QueueDropInflight;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.Scanner;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.yammer.metrics.core.MetricName;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Implementation of an {@link com.continuuity.data.operation.executor.OperationExecutor}
 * that executes all operations within Omid-style transactions.
 *
 * See https://github.com/yahoo/omid/ for more information on the Omid design.
 */
@Singleton
public class OmidTransactionalOperationExecutor
  implements TransactionalOperationExecutor {

  private static final Logger Log
    = LoggerFactory.getLogger(OmidTransactionalOperationExecutor.class);

  public String getName() {
    return "omid(" + tableHandle.getName() + ")";
  }

  /**
   * The Transaction Oracle used by this executor instance.
   */
  TransactionOracle oracle;

  /**
   * The {@link OVCTableHandle} handle used to get references to tables.
   */
  OVCTableHandle tableHandle;

  private OrderedVersionedColumnarTable metaTable;
  private OrderedVersionedColumnarTable randomTable;

  private MetaDataStore metaStore;

  private TTQueueTable queueTable;
  private TTQueueTable streamTable;

  public static boolean disableQueuePayloads = false;

  static int maxDequeueRetries = 200;
  static long dequeueRetrySleep = 5;

  // Defines whether this opex talks to oracle or not. With current implementation if set to false the only difference
  // is that transaction validation doesn't happen through oracle.
  // TODO: Ideally we should also forbid oracle-related ops, like starting/aborting transactions, but now too many
  //       things (like accessing meta table, etc.) mixed in this sinlge class which makes it hard to do.
  @Inject(optional = true)
  @Named("DataFabricOperationExecutorTalksToOracle")
  private boolean talkToOracle = true;

  // A proxy that runs all queue operations while managing state.
  // Also runs all queue operations for a single consumer serially.
  private final QueueStateProxy queueStateProxy;

  /** Min table write ops to attempt to batch */
  private final int minTableWriteOpsToBatch;

  @Inject
  public OmidTransactionalOperationExecutor(TransactionOracle oracle, OVCTableHandle tableHandle, CConfiguration conf) {
    this.oracle = oracle;
    this.tableHandle = tableHandle;
    this.minTableWriteOpsToBatch = conf.getInt(Constants.CFG_DATA_TABLE_WRITE_OPS_BATCH_MIN_SIZE,
                                               Constants.DEFAULT_DATA_TABLE_WRITE_OPS_BATCH_MIN_SIZE);
    // Default cache size is 200 MB
    queueStateProxy = new QueueStateProxy(
      conf.getLongBytes(Constants.CFG_QUEUE_STATE_PROXY_MAX_CACHE_SIZE_BYTES,
                        Constants.DEAFULT_CFG_QUEUE_STATE_PROXY_MAX_CACHE_SIZE_BYTES));
  }

  // Metrics

  /* -------------------  data fabric system metrics ---------------- */
  private CMetrics cmetric = new CMetrics(MetricType.System);

  private static final String METRIC_PREFIX = "omid-opex-";

  public static final String NUMOPS_METRIC_SUFFIX = "-numops";
  public static final String REQ_TYPE_GET_SPLITS_NUM_OPS = METRIC_PREFIX + "GetSplits" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_READ_ALL_KEYS_NUM_OPS = METRIC_PREFIX + "ReadAllKeys" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_READ_NUM_OPS = METRIC_PREFIX + "Read" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_READ_COLUMN_RANGE_NUM_OPS =
    METRIC_PREFIX + "ReadColumnRange" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_CLEAR_FABRIC_NUM_OPS = METRIC_PREFIX + "ClearFabric" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_WRITE_OPERATION_BATCH_NUM_OPS =
    METRIC_PREFIX + "WriteOperationBatch" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_WRITE_NUM_OPS = METRIC_PREFIX + "Write" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_DELETE_NUM_OPS = METRIC_PREFIX + "Delete" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_INCREMENT_NUM_OPS = METRIC_PREFIX + "Increment" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_COMPARE_AND_SWAP_NUM_OPS =
    METRIC_PREFIX + "CompareAndSwap" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_QUEUE_ENQUEUE_NUM_OPS = METRIC_PREFIX + "QueueEnqueue" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_QUEUE_ACK_NUM_OPS = METRIC_PREFIX + "QueueAck" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_QUEUE_DEQUEUE_NUM_OPS = METRIC_PREFIX + "QueueDequeue" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_GET_GROUP_ID_NUM_OPS = METRIC_PREFIX + "GetGroupID" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_GET_QUEUE_INFO_NUM_OPS = METRIC_PREFIX + "GetQueueInfo" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_START_TRANSACTION_NUM_OPS =
    METRIC_PREFIX + "StartTransaction" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_COMMIT_TRANSACTION_NUM_OPS =
    METRIC_PREFIX + "CommitTransaction" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_QUEUE_CONFIGURE_NUM_OPS =
    METRIC_PREFIX + "QueueConfigure" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_QUEUE_CONFIGURE_GRPS_NUM_OPS =
    METRIC_PREFIX + "QueueConfigureGroups" + NUMOPS_METRIC_SUFFIX;
  public static final String REQ_TYPE_QUEUE_DROP_INFLIGHT_OPS =
    METRIC_PREFIX + "QueueDropInflight" + NUMOPS_METRIC_SUFFIX;
  private static final String REQ_TYPE_SCAN_NUM_OPS = METRIC_PREFIX + "Scan" + NUMOPS_METRIC_SUFFIX;

  public static final String LATENCY_METRIC_SUFFIX = "-latency";
  public static final String REQ_TYPE_GET_SPLITS_LATENCY = METRIC_PREFIX + "GetSplits" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_READ_ALL_KEYS_LATENCY = METRIC_PREFIX + "ReadAllKeys" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_READ_LATENCY = METRIC_PREFIX + "Read" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_READ_COLUMN_RANGE_LATENCY =
    METRIC_PREFIX + "ReadColumnRange" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_CLEAR_FABRIC_LATENCY = METRIC_PREFIX + "ClearFabric" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_WRITE_OPERATION_BATCH_LATENCY =
    METRIC_PREFIX + "WriteOperationBatch" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_WRITE_LATENCY = METRIC_PREFIX + "Write" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_DELETE_LATENCY = METRIC_PREFIX + "Delete" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_INCREMENT_LATENCY = METRIC_PREFIX + "Increment" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_COMPARE_AND_SWAP_LATENCY =
    METRIC_PREFIX + "CompareAndSwap" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_QUEUE_ENQUEUE_LATENCY = METRIC_PREFIX + "QueueEnqueue" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_QUEUE_ACK_LATENCY = METRIC_PREFIX + "QueueAck" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_QUEUE_DEQUEUE_LATENCY = METRIC_PREFIX + "QueueDequeue" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_GET_GROUP_ID_LATENCY = METRIC_PREFIX + "GetGroupID" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_GET_QUEUE_INFO_LATENCY = METRIC_PREFIX + "GetQueueInfo" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_QUEUE_CONFIGURE_LATENCY =
    METRIC_PREFIX + "QueueConfigure" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_QUEUE_CONFIGURE_GRPS_LATENCY =
    METRIC_PREFIX + "QueueConfigureGroups" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_QUEUE_DROP_INFLIGHT_LATENCY =
    METRIC_PREFIX + "QueueDropInflight" + LATENCY_METRIC_SUFFIX;
  public static final String REQ_TYPE_SCAN_LATENCY = METRIC_PREFIX + "Scan" + LATENCY_METRIC_SUFFIX;

  private void incMetric(String metric) {
    cmetric.meter(metric, 1);
  }

  private void incMetric(String metric, long delta) {
    cmetric.meter(metric, delta);
  }

  private long begin() {
    return System.currentTimeMillis();
  }

  private void end(String metric, long beginning) {
    cmetric.histogram(metric, System.currentTimeMillis() - beginning);
  }

  private void end(String metric, long beginning, int opsNum) {
    cmetric.histogram(metric, (System.currentTimeMillis() - beginning) / opsNum);
  }

  /* -------------------  (interstitial) queue metrics ---------------- */
  private ConcurrentMap<String, CMetrics> queueMetrics =
      new ConcurrentHashMap<String, CMetrics>();

  private CMetrics getQueueMetric(String group) {
    CMetrics metric = queueMetrics.get(group);
    if (metric == null) {
      queueMetrics.putIfAbsent(group,
          new CMetrics(MetricType.FlowSystem, group));
      metric = queueMetrics.get(group);
      Log.trace("Created new CMetrics for group '" + group + "'.");
      // System.err.println("Created new CMetrics for group '" + group + "'.");
    }
    return metric;
  }

  private ConcurrentMap<byte[], ImmutablePair<String, String>>
      queueMetricNames = new ConcurrentSkipListMap<byte[],
      ImmutablePair<String, String>>(Bytes.BYTES_COMPARATOR);

  private ImmutablePair<String, String> getQueueMetricNames(byte[] queue) {
    ImmutablePair<String, String> names = queueMetricNames.get(queue);
    if (names == null) {
      String name = new String(queue).replace(":", "");
      queueMetricNames.putIfAbsent(queue, new ImmutablePair<String, String>
          ("q.enqueue." + name, "q.ack." + name));
      names = queueMetricNames.get(queue);
      Log.trace("using metric name '" + names.getFirst() + "' and '"
          + names.getSecond() + "' for queue '" + new String(queue) + "'");
      //System.err.println("using metric name '" + names.getFirst() + "' and '"
      //    + names.getSecond() + "' for queue '" + new String(queue) + "'");
    }
    return names;
  }

  private void enqueueMetric(byte[] queue, QueueProducer producer) {
    if (producer != null && producer.getProducerName() != null) {
      String metricName = getQueueMetricNames(queue).getFirst();
      getQueueMetric(producer.getProducerName()).meter(metricName, 1);
    }
  }

  private void ackMetric(byte[] queue, QueueConsumer consumer) {
    if (consumer != null && consumer.getGroupName() != null) {
      String metricName = getQueueMetricNames(queue).getSecond();
      getQueueMetric(consumer.getGroupName()).meter(metricName, 1);
    }
  }


  /* -------------------  (global) stream metrics ---------------- */
  private CMetrics streamMetric = // we use a global flow group
      new CMetrics(MetricType.FlowSystem, "-.-.-.-.-.0");

  private ConcurrentMap<byte[], ImmutablePair<String, String>>
      streamMetricNames = new ConcurrentSkipListMap<byte[],
      ImmutablePair<String, String>>(Bytes.BYTES_COMPARATOR);

  private ImmutablePair<String, String> getStreamMetricNames(byte[] stream) {
    ImmutablePair<String, String> names = streamMetricNames.get(stream);
    if (names == null) {
      String name = new String(stream).replace(":", "");
      streamMetricNames.putIfAbsent(stream, new ImmutablePair<String, String>(
        "stream.enqueue." + name, "stream.storage." + name));
      names = streamMetricNames.get(stream);
      Log.trace("using metric name '" + names.getFirst() + "' and '"
          + names.getSecond() + "' for stream '" + new String(stream) + "'");
      //System.err.println("using metric name '" + names.getFirst() + "' and '"
      //    + names.getSecond() + "' for stream '" + new String(stream) + "'");
    }
    return names;
  }

  private boolean isStream(byte[] queueName) {
    return Bytes.startsWith(queueName, TTQueue.STREAM_NAME_PREFIX);
  }

  private int streamSizeEstimate(byte[] streamName, int dataSize, int numEntries) {
    // assume HBase uses space for the stream name, the data, and some metadata
    return dataSize + numEntries * (streamName.length + 50);
  }

  private void streamMetric(byte[] streamName, int dataSize, int numEntries) {
    ImmutablePair<String, String> names = getStreamMetricNames(streamName);
    streamMetric.meter(names.getFirst(), 1);
    streamMetric.meter(names.getSecond(), streamSizeEstimate(streamName, dataSize, numEntries));
  }

  // By using this we reduce amount of strings to concat for super-freq operations, which (shown in tests) reduces
  // mem allocation by at least 10% and cpu time by at least 10% at the moment of change
  private CMetrics dataSetReadMetric = // we use a global flow group
    new CMetrics(MetricType.FlowSystem, "-.-.-.-.-.0") {
      @Override
      protected MetricName getMetricName(Class<?> scope, String metricName) {
        return super.getMetricName(scope, "dataset.read." + metricName);
      }
    };

  private CMetrics dataSetWriteMetric = // we use a global flow group
    new CMetrics(MetricType.FlowSystem, "-.-.-.-.-.0") {
      @Override
      protected MetricName getMetricName(Class<?> scope, String metricName) {
        return super.getMetricName(scope, "dataset.write." + metricName);
      }
    };

  private CMetrics dataSetStorageMetric = // we use a global flow group
    new CMetrics(MetricType.FlowSystem, "-.-.-.-.-.0") {
      @Override
      protected MetricName getMetricName(Class<?> scope, String metricName) {
        return super.getMetricName(scope, "dataset.storage." + metricName);
      }
    };

  private void dataSetMetric_read(String dataSetName) {
    // note: we intentionally do not provide table name for some system operations (like talking to MDS) so that
    //       we can skip writing metrics here. Yes, this looks like a hack. Should be fixed with new metrics system.
    if (dataSetName != null) {
      dataSetReadMetric.meter(dataSetName, 1);
    }
  }

  private void dataSetMetric_write(String dataSetName, int dataSize) {
    // note: we intentionally do not provide table name for some system operations (like talking to MDS) so that
    //       we can skip writing metrics here. Yes, this looks like a hack. Should be fixed with new metrics system.
    if (dataSetName != null) {
      dataSetWriteMetric.meter(dataSetName, 1);
      dataSetStorageMetric.meter(dataSetName, dataSize);
    }
  }
  
  /* -------------------  end metrics ---------------- */

  // named table management

  // a map of logical table name to existing <real name, table>, caches
  // the meta data store and the ovc table handle
  // there are three possible states for a table:
  // 1. table does not exist or is not known -> no entry
  // 2. table is being created -> entry with real name, but null for the table
  // 3. table is known -> entry with name and table
  ConcurrentMap<ImmutablePair<String, String>,
      ImmutablePair<byte[], OrderedVersionedColumnarTable>> namedTables;

  // method to find - and if necessary create - a table
  OrderedVersionedColumnarTable findRandomTable(OperationContext context, String name) throws OperationException {

    // check whether it is one of the default tables these are always
    // pre-loaded at initializaton and we can just return them
    if (null == name) {
      return this.randomTable;
    }
    if ("meta".equals(name)) {
      return this.metaTable;
    }

    // look up table in in-memory map. if this returns:
    // an actual name and OVCTable, return that OVCTable
    ImmutablePair<String, String> tableKey = new ImmutablePair<String, String>(context.getAccount(), name);
    ImmutablePair<byte[], OrderedVersionedColumnarTable> nameAndTable = this.namedTables.get(tableKey);
    if (nameAndTable != null) {
      if (nameAndTable.getSecond() != null) {
        return nameAndTable.getSecond();
      }

      // an actual name and null for the table, then sleep/repeat until the look
      // up returns non-null for the table. This is the case when some other
      // thread in the same process has generated an actual name and is in the
      // process of creating that table.
      return waitForTableToMaterialize(tableKey);
    }
    // null: then this table has not been opened by any thread in this
    // process. In this case:
    // Read the meta data for the logical name from MDS.
    MetaDataEntry meta = this.metaStore.get(
        context, context.getAccount(), null, "namedTable", name);
    if (null != meta) {
      return waitForTableToMaterializeInMeta(context, name, meta);

    // Null: Nobody has started to create this.
    } else {
      // Generate a new actual name, and write that name with status Pending
      // to MDS in a Compare-and-Swap operation
      byte[] actualName = generateActualName(context, name);
      MetaDataEntry newMeta = new MetaDataEntry(context.getAccount(), null,
          "namedTable", name);
      newMeta.addField("actual", actualName);
      newMeta.addField("status", "pending");
      try {
        this.metaStore.add(context, newMeta);
      } catch (OperationException e) {
        if (e.getStatus() == StatusCode.WRITE_CONFLICT) {
          // If C-a-S failed with write conflict, then some other process (or
          // thread) has concurrently attempted the same and wins.
          return waitForTableToMaterializeInMeta(context, name, newMeta);
        } else {
          throw e;
        }
      }
      //C-a-S succeeded, add <actual name, null> to MEM to inform other threads
      //in this process to wait (no other thread could have updated in the
      //    mean-time without updating MDS)
      this.namedTables.put(tableKey,
          new ImmutablePair<byte[], OrderedVersionedColumnarTable>(
              actualName, null));

      //Create a new actual table for the actual name. This should never fail.
      OrderedVersionedColumnarTable table =
          getTableHandle().getTable(actualName);

      // Update MDS with the new status Ready. This can be an ordinary Write
      newMeta.addField("status", "ready");
      this.metaStore.update(context, newMeta);

      // because all others are waiting.
      // Update MEM with the actual created OVCTable.
      this.namedTables.put(tableKey,
          new ImmutablePair<byte[], OrderedVersionedColumnarTable>(
              actualName, table));
      //Return the created table.
      return table;
    }
  }

  private byte[] generateActualName(OperationContext context, String name) {
    // TODO make this generate a new id every time it is called
    return ("random_" + context.getAccount() + "_" + name).getBytes();
  }

  private OrderedVersionedColumnarTable waitForTableToMaterialize(ImmutablePair<String, String> tableKey) {
    while (true) {
      ImmutablePair<byte[], OrderedVersionedColumnarTable> nameAndTable =
          this.namedTables.get(tableKey);
      if (nameAndTable == null) {
        throw new RuntimeException("In-memory entry went from non-null to null " +
            "for named table \"" + tableKey.getSecond() + "\"");
      }
      if (nameAndTable.getSecond() != null) {
        return nameAndTable.getSecond();
      }
      // sleep a little
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        // what the heck should I do?
      }
    }
    // TODO should this time out after some time or number of attempts?
  }

  OrderedVersionedColumnarTable waitForTableToMaterializeInMeta(OperationContext context,
                                                                String name,
                                                                MetaDataEntry meta)
    throws OperationException {

    while (true) {
      // If this returns: An actual name and status Ready: The table is ready
      // to use, open the table, add it to MEM and return it
      if ("ready".equals(meta.getTextField("status"))) {
        byte[] actualName = meta.getBinaryField("actual");
        if (actualName == null) {
          throw new RuntimeException("Encountered meta data entry of type " +
                                       "\"namedTable\" without actual name for table name \"" +
                                       name + "\".");
        }
        OrderedVersionedColumnarTable table = getTableHandle().getTable(actualName);
        if (table == null) {
          throw new RuntimeException("table handle \"" + getTableHandle().getName() + "\": getTable returned null for" +
                                       " actual table name " + "\"" + new String(actualName) + "\"");
        }

        // update MEM. This can be ordinary put, because even if some other
        // thread updated it in the meantime, it would have put the same table.
        ImmutablePair<String, String> tableKey = new ImmutablePair<String, String>(context.getAccount(), name);
        this.namedTables.put(tableKey, new ImmutablePair<byte[], OrderedVersionedColumnarTable>(actualName, table));
        return table;

      } else if (!"pending".equals(meta.getTextField("status"))) {
        // An actual name and status Pending: The table is being created. Loop
        // and repeat MDS read until status is Ready and see previous case
        throw new RuntimeException("Found meta data entry with unkown status " +
            Objects.toStringHelper(meta.getTextField("status")));
      }

      // sleep a little
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        // what the heck should I do?
      }

      // reread the meta data, hopefully it has changed to ready
      meta = this.metaStore.get(
          context, context.getAccount(), null, "namedTable", name);
      if (meta == null) {
        // this should never happen - we only enter this method in two cases:
        // 1. there is already an entry, but it might be pending
        // 2. we encountered a read conflict when attempting to create it
        //    - hence this get must see that conflicting write
        // TODO what if someone deleted it after the write conflict happened?
        // TODO what if the write conflict was caused by a delete?
        throw new RuntimeException("Meta data entry went from non-null to null " +
            "for table \"" + name + "\"");
      }
      // TODO should this time out after some time or number of attempts?
    }
  }

  // Single reads

  @Override
  public OperationResult<List<byte[]>> execute(OperationContext context,
                                               ReadAllKeys readKeys)
    throws OperationException {
    return execute(context, null, readKeys);
  }

  @Override
  public OperationResult<List<byte[]>> execute(OperationContext context,
                                               Transaction transaction,
                                               ReadAllKeys readKeys)
    throws OperationException {

    initialize();
    incMetric(REQ_TYPE_READ_ALL_KEYS_NUM_OPS);
    long begin = begin();
    OrderedVersionedColumnarTable table = this.findRandomTable(context, readKeys.getTable());
    ReadPointer pointer = getReadPointer(transaction);
    List<byte[]> result = table.getKeys(readKeys.getLimit(), readKeys.getOffset(), pointer);
    end(REQ_TYPE_READ_ALL_KEYS_LATENCY, begin);
    dataSetMetric_read(readKeys.getMetricName());
    return new OperationResult<List<byte[]>>(result);
  }

  @Override
  public OperationResult<List<KeyRange>> execute(OperationContext context,
                                                 GetSplits getSplits)
    throws OperationException {
    return execute(context, null, getSplits);
  }

  @Override
  public OperationResult<List<KeyRange>> execute(OperationContext context,
                                                 @Nullable Transaction transaction,
                                                 GetSplits getSplits)
    throws OperationException {

    initialize();
    incMetric(REQ_TYPE_GET_SPLITS_NUM_OPS);
    long begin = begin();
    OrderedVersionedColumnarTable table = this.findRandomTable(context, getSplits.getTable());
    ReadPointer pointer = getReadPointer(transaction);
    List<KeyRange> result = table.getSplits(getSplits.getNumSplits(), getSplits.getStart(), getSplits.getStop(),
                                            getSplits.getColumns(), pointer);
    end(REQ_TYPE_GET_SPLITS_LATENCY, begin);
    dataSetMetric_read(getSplits.getMetricName());
    return new OperationResult<List<KeyRange>>(result);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      Read read)
    throws OperationException {
    return execute(context, null, read);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      Transaction transaction,
                                                      Read read)
    throws OperationException {
    initialize();
    incMetric(REQ_TYPE_READ_NUM_OPS);
    long begin = begin();
    OrderedVersionedColumnarTable table = this.findRandomTable(context, read.getTable());
    ReadPointer pointer = getReadPointer(transaction);
    OperationResult<Map<byte[], byte[]>> result =
      table.get(read.getKey(), read.getColumns(), pointer);
    end(REQ_TYPE_READ_LATENCY, begin);
    dataSetMetric_read(read.getMetricName());
    return result;
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      ReadColumnRange readColumnRange)
    throws OperationException {
    return execute(context, null, readColumnRange);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      Transaction transaction,
                                                      ReadColumnRange readColumnRange)
    throws OperationException {

    initialize();
    incMetric(REQ_TYPE_READ_COLUMN_RANGE_NUM_OPS);
    long begin = begin();
    OrderedVersionedColumnarTable table = this.findRandomTable(context, readColumnRange.getTable());
    ReadPointer pointer = getReadPointer(transaction);
    OperationResult<Map<byte[], byte[]>> result = table.get(readColumnRange.getKey(),
                                                            readColumnRange.getStartColumn(),
                                                            readColumnRange.getStopColumn(),
                                                            readColumnRange.getLimit(), pointer);
    end(REQ_TYPE_READ_COLUMN_RANGE_LATENCY, begin);
    dataSetMetric_read(readColumnRange.getMetricName());
    return result;
  }

  // Administrative calls

  @Override
  public void execute(OperationContext context,
                      ClearFabric clearFabric) throws OperationException {
    initialize();
    incMetric(REQ_TYPE_CLEAR_FABRIC_NUM_OPS);
    long begin = begin();
    if (clearFabric.shouldClearData()) {
      this.randomTable.clear();
    }
    if (clearFabric.shouldClearTables()) {
      List<MetaDataEntry> entries = this.metaStore.list(context, context.getAccount(), null, "namedTable", null);
      for (MetaDataEntry entry : entries) {
        String name = entry.getId();
        OrderedVersionedColumnarTable table = findRandomTable(context, name);
        table.clear();
        this.namedTables.remove(new ImmutablePair<String, String>(context.getAccount(), name));
        this.metaStore.delete(context, entry.getAccount(), entry.getApplication(), entry.getType(), entry.getId());
      }
    }
    if (clearFabric.shouldClearMeta()) {
      this.metaTable.clear();
    }
    if (clearFabric.shouldClearQueues()) {
      this.queueTable.clear();
    }
    if (clearFabric.shouldClearStreams()) {
      this.streamTable.clear();
    }
    end(REQ_TYPE_CLEAR_FABRIC_LATENCY, begin);
  }

  @Override
  public void execute(OperationContext context, OpenTable openTable)
    throws OperationException {
    initialize();
    findRandomTable(context, openTable.getTableName());
  }

  // Write batches

  @Override
  public void commit(OperationContext context, List<WriteOperation> writes)
    throws OperationException {
    initialize();
    incMetric(REQ_TYPE_WRITE_OPERATION_BATCH_NUM_OPS);
    long begin = begin();
    cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_NumReqs", writes.size());
    // when transaction is not defined, we start tx that tracks changes by default
    commit(context, startTransaction(true), writes);
    end(REQ_TYPE_WRITE_OPERATION_BATCH_LATENCY, begin);
  }

  @Override
  public Transaction startTransaction(OperationContext context, boolean trackChanges)
    throws OperationException {
    return this.startTransaction(trackChanges);
  }

  @Override
  public Transaction execute(OperationContext context,
                             Transaction transaction,
                             List<WriteOperation> writes) throws OperationException {
    // make sure we have a valid transaction
    if (transaction != null) {
      validateTransaction(transaction);
    } else {
      // when transaction is not defined, we start tx that tracks changes by default
      transaction = startTransaction(true);
    }

    // TODO should we add an empty batch of undos to the transaction in oracle?
    // TODO That would update the timestamp of the transaction and prevent it from time out
    if (writes.isEmpty()) {
      return transaction;
    }

    // Re-order operations (create a copy for now)
    List<WriteOperation> orderedWrites = new ArrayList<WriteOperation>(writes);
    Collections.sort(orderedWrites, new WriteOperationComparator());

    List<Undo> undos = new ArrayList<Undo>(writes.size());

    // Execute all operations
    // First execute all table ops and then non-table (like queue ops) ops (to allow for optimizing them). It is OK
    // to do this in different order form original as the ops are executed on different tables
    // NOTE: executing queue ops later allows us to have better performance because it is hard to undo them.
    int tableOps = 0;
    WriteTransactionResult writeTxReturn = null;
    // buffer table write ops for later execution
    Map<String, List<WriteOperation>> tableWriteOperations = Maps.newHashMap();
    List<WriteOperation> nonTableOps = Lists.newArrayList();
    for (WriteOperation write : orderedWrites) {
      if (write instanceof TableOperation) {
        add(tableWriteOperations, write);
        tableOps++;
        // we will do them later (it is ok to do them in out-of-order with non-table ops)
        continue;
      }

      nonTableOps.add(write);
    }

    // Dispatching table write ops
    boolean abort = false;
    if (tableOps < minTableWriteOpsToBatch) {
      writeTxReturn = dispatchTableWritesInBatches(context, transaction, tableWriteOperations);
    } else {
      writeTxReturn = dispatchTableWrites(context, transaction, tableWriteOperations);
    }
    if (!writeTxReturn.success) {
      // Write operations failed
      abort = true;
    }
    // adding undos if there are any
    undos.addAll(writeTxReturn.undos);

    // Dispatching non-table ops
    if (!abort) {
      for (WriteOperation write : nonTableOps) {
        writeTxReturn = dispatchWrite(context, write, transaction);

        undos.addAll(writeTxReturn.undos);
        if (!writeTxReturn.success) {
          // Write operation failed
          cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_FailedWrites", 1);
          abort = true;
          break;
        }
      }
    }

    // whether success or not, we must notify the oracle of all operations
    if (!undos.isEmpty()) {
      addToTransaction(transaction, undos);
    }

    // if any write failed, abort the transaction
    if (abort) {
      // automatically abort transaction if it is not long-running one (ie. tracking one). In non-tracking case, it is
      // assumed that tx is always managed by tx agent.
      if (transaction.isTrackChanges()) {
        abort(context, transaction);
      }
      throw new OmidTransactionException(
        writeTxReturn.statusCode, writeTxReturn.message);
    }
    return transaction;
  }

  private WriteTransactionResult dispatchTableWrites(OperationContext context, Transaction transaction,
                                                     Map<String, List<WriteOperation>> tableWriteOperations)
    throws OperationException {
    List<Undo> undos = new ArrayList<Undo>(tableWriteOperations.size());
    for (Map.Entry<String, List<WriteOperation>> tableOps : tableWriteOperations.entrySet()) {
      WriteTransactionResult writeTxReturn;
      for (WriteOperation write : tableOps.getValue()) {
        writeTxReturn = dispatchWrite(context, write, transaction);
        // whether success or not, we must notify the oracle of all operations
        undos.addAll(writeTxReturn.undos);
        if (!writeTxReturn.success) {
          // Write operation failed
          cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_FailedWrites", 1);
          return new WriteTransactionResult(false, writeTxReturn.statusCode, writeTxReturn.message, undos);
        }
      }
    }
    return new WriteTransactionResult(undos);
  }

  private WriteTransactionResult dispatchTableWritesInBatches(OperationContext context, Transaction transaction,
                                                     Map<String, List<WriteOperation>> tableWriteOperations)
    throws OperationException {

    List<Undo> undos = new ArrayList<Undo>(tableWriteOperations.size());
    for (Map.Entry<String, List<WriteOperation>> tableOps : tableWriteOperations.entrySet()) {
      // NOTE: Currently we batch only write ops. Potentially we should also batch at least no-read incs.
      //       Ideally other ops too.
      //       This is not the most efficient way to batch operations, but should be OK for now.
      WriteTransactionResult writeTxReturn;
      List<Write> writeOps = Lists.newArrayList();

      for (WriteOperation write : tableOps.getValue()) {
        if (write instanceof Write) {
          writeOps.add((Write) write);
          continue;
        }

        // Writing accumulated writeOps
        if (writeOps.size() > 0) {
          writeTxReturn = write(context, transaction, writeOps, undos);
          if (!writeTxReturn.success) {
            return writeTxReturn;
          }
          writeOps.clear();
        }

        // Writing other operations
        writeTxReturn = dispatchWrite(context, write, transaction);

        if (!writeTxReturn.success) {
          // Write operation failed
          cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_FailedWrites", 1);
          return new WriteTransactionResult(false, writeTxReturn.statusCode, writeTxReturn.message, undos);
        } else {
          // Write was successful.  Store undo if we need to abort and continue
          undos.addAll(writeTxReturn.undos);
        }
      }

      // Writing accumulated but not written writeOps
      if (writeOps.size() > 0) {
        writeTxReturn = write(context, transaction, writeOps, undos);
        if (!writeTxReturn.success) {
          return writeTxReturn;
        }
      }
    }

    return new WriteTransactionResult(undos);
  }

  private WriteTransactionResult write(OperationContext context, Transaction transaction,
                                       List<Write> writeOps, List<Undo> undos) throws OperationException {
    WriteTransactionResult writeTxReturn = writeWrites(context, writeOps, transaction);
    if (!writeTxReturn.success) {
      // write operation failed
      cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_FailedWrites", writeOps.size());
      // stop on failure
      return new WriteTransactionResult(false, writeTxReturn.statusCode, writeTxReturn.message, undos);
    }
    // Writes were successful.  Store undo if we need to abort and continue
    undos.addAll(writeTxReturn.undos);
    return writeTxReturn;
  }

  private void add(Map<String, List<WriteOperation>> tableWriteOperations,
                   WriteOperation write) {
    String table = ((TableOperation) write).getTable();
    List<WriteOperation> tableOps = tableWriteOperations.get(table);
    if (tableOps == null) {
      tableOps = Lists.newArrayList();
      tableWriteOperations.put(table, tableOps);
    }
    tableOps.add(write);
  }

  @Override
  public void commit(OperationContext context,
                     Transaction transaction)
    throws OperationException {

    // attempt to commit in Oracle
    TransactionResult txResult = commitTransaction(transaction);
    if (!txResult.isSuccess()) {
      // make sure to emit the metric for failed commits
      cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_FailedCommits", 1);

      // attempt to undo all the writes of the transaction
      // (transaction is already marked as invalid in oracle)
      attemptUndo(context, transaction, txResult.getUndos());

      throw new OmidTransactionException(StatusCode.WRITE_CONFLICT,
                                         "Commit of transaction failed, transaction aborted");
    }
    // Commit was successful.

    // TODO this must go away with the new queue implementation
    // If the transaction did a queue ack, finalize it
    QueueFinalize finalize = txResult.getFinalize();
    if (finalize != null) {
      finalize.execute(queueStateProxy, getQueueTable(finalize.getQueueName()), transaction);
    }

    // emit metrics for the transaction and the queues/streams involved
    cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_SuccessfulTransactions", 1);
    // for each queue operation (enqueue or ack)
    for (Undo undo : txResult.getUndos()) {
      if (undo instanceof QueueUndo.QueueUnenqueue) {
        QueueUndo.QueueUnenqueue unenqueue = (QueueUndo.QueueUnenqueue) undo;
        QueueProducer producer = unenqueue.producer;
        enqueueMetric(unenqueue.queueName, producer);
        if (isStream(unenqueue.queueName)) {
          streamMetric(unenqueue.queueName, unenqueue.sumOfSizes, unenqueue.numEntries());
        }
      } else if (undo instanceof QueueUndo.QueueUnack) {
        QueueUndo.QueueUnack unack = (QueueUndo.QueueUnack) undo;
        QueueConsumer consumer = unack.consumer;
        ackMetric(unack.queueName, consumer);
      }
    }
    // done
  }

  @Override
  public void commit(OperationContext context,
                     Transaction transaction,
                     List<WriteOperation> writes) throws OperationException {
    transaction = execute(context, transaction, writes);
    commit(context, transaction);
  }

  @Override
  public void abort(OperationContext context, Transaction transaction) throws OperationException {
    // abort transaction in oracle, that returns the undos to be performed
    TransactionResult txResult = abortTransaction(transaction);
    // attempt to ubdo all the writes of the transaction
    // (transaction is already marked as invalid in oracle)
    attemptUndo(context, transaction, txResult.getUndos());
  }

  @Override
  public Map<byte[], Long> increment(OperationContext context,
                                     Increment increment) throws
    OperationException {
    // start transaction, execute increment, commit transaction, return result

    // when transaction is not defined, we start tx that tracks changes by default
    Transaction tx = startTransaction(true);
    Map<byte[], Long> result = increment(context, tx, increment);
    commit(context, tx);
    return result;
  }

  @Override
  public Map<byte[], Long> increment(OperationContext context,
                                     Transaction transaction,
                                     Increment increment) throws OperationException {
    // if a null transaction is passed in,
    if (transaction == null) {
      throw new OmidTransactionException(StatusCode.INVALID_TRANSACTION, "transaction cannot be null");
    } else {
      validateTransaction(transaction);
    }

    WriteTransactionResult writeTxReturn = write(context, increment, transaction);
    List<Undo> undos = writeTxReturn.undos;
    // whether success or not, we must notify the oracle of all operations
    if (null != undos && !undos.isEmpty()) {
      addToTransaction(transaction, undos);
    }
    if (writeTxReturn.success) {
      // increment was successful. the return value is in the write transaction result
      return writeTxReturn.incrementResult;
    } else {
      // operation failed
      cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_FailedWrites", 1);
      // automatically abort transaction if it is not long-running one (ie. tracking one). In non-tracking case, it is
      // assumed that tx is always managed by tx agent.
      if (transaction.isTrackChanges()) {
        abort(context, transaction);
      }
      throw new OmidTransactionException(writeTxReturn.statusCode, writeTxReturn.message);
    }
  }

  public OVCTableHandle getTableHandle() {
    return this.tableHandle;
  }

  static final List<Undo> NO_UNDOS = Collections.emptyList();

  class WriteTransactionResult {
    final boolean success;
    final int statusCode;
    final String message;
    final List<Undo> undos;
    Map<byte[], Long> incrementResult;

    WriteTransactionResult(boolean success, int status, String message, List<Undo> undos) {
      this.success = success;
      this.statusCode = status;
      this.message = message;
      this.undos = undos;
    }

    // successful, multiple deletes to undo
    WriteTransactionResult(List<Undo> undos) {
      this(true, StatusCode.OK, null, undos);
    }

    // successful, one delete to undo
    WriteTransactionResult(Undo undo) {
      this(true, StatusCode.OK, null, Collections.singletonList(undo));
    }

    // successful increment, one delete to undo
    WriteTransactionResult(Undo undo, Map<byte[], Long> incrementResult) {
      this(true, StatusCode.OK, null, Collections.singletonList(undo));
      this.incrementResult = incrementResult;
    }

    // failure with status code and message, nothing to undo
    WriteTransactionResult(int status, String message) {
      this(false, status, message, NO_UNDOS);
    }
  }

  /**
   * Actually perform the various write operations.
   */
  private WriteTransactionResult dispatchWrite(
      OperationContext context, WriteOperation write,
      Transaction transaction) throws OperationException {
    if (write instanceof Write) {
      return write(context, (Write) write, transaction);
    } else if (write instanceof Delete) {
      return write(context, (Delete) write, transaction);
    } else if (write instanceof Increment) {
      return write(context, (Increment) write, transaction);
    } else if (write instanceof CompareAndSwap) {
      return write(context, (CompareAndSwap) write, transaction);
    } else if (write instanceof QueueEnqueue) {
      return write((QueueEnqueue) write, transaction);
    } else if (write instanceof QueueAck) {
      return write((QueueAck) write, transaction);
    }
    return new WriteTransactionResult(StatusCode.INTERNAL_ERROR,
        "Unknown write operation " + write.getClass().getName());
  }

  WriteTransactionResult write(OperationContext context, Write write, Transaction transaction)
    throws OperationException {
    initialize();
    incMetric(REQ_TYPE_WRITE_NUM_OPS);
    long begin = begin();
    OrderedVersionedColumnarTable table = this.findRandomTable(context, write.getTable());
    table.put(write.getKey(), write.getColumns(), transaction.getWriteVersion(), write.getValues());
    end(REQ_TYPE_WRITE_LATENCY, begin);
    dataSetMetric_write(write.getMetricName(), write.getSize());
//    return new WriteTransactionResult(new Delete(write.getTable(), write.getKey(), write.getColumns()));
    return new WriteTransactionResult(new UndoWrite(write.getTable(), write.getKey(), write.getColumns()));
  }

  WriteTransactionResult writeWrites(OperationContext context, List<Write> writes, Transaction transaction)
    throws OperationException {
    initialize();
    incMetric(REQ_TYPE_WRITE_NUM_OPS, writes.size());
    long begin = begin();

    // First, group ops by row key and reduce if needed (only latest write to the cell is kept)

    // NOTE: all writes operate on same table.
    String tableName = writes.get(0).getTable();
    OrderedVersionedColumnarTable table = this.findRandomTable(context, tableName);
    Map<byte[], Map<byte[], byte[]>> opsByKey = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Write write : writes) {
      Map<byte[], byte[]> colVals = opsByKey.get(write.getKey());
      if (colVals == null) {
        colVals = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        opsByKey.put(write.getKey(), colVals);
      }
      for (int i = 0; i < write.getColumns().length; i++) {
        // NOTE: as we walk thru ops in a order of their expected execution we don't care if we override
        //       value originated in older write by newer one.
        colVals.put(write.getColumns()[i], write.getValues()[i]);
      }
    }


    // Second, write to the table

    byte[][] keys = new byte[opsByKey.size()][];
    byte[][][] cols = new byte[keys.length][][];
    byte[][][] vals = new byte[keys.length][][];
    List<Undo> undos = Lists.newArrayList();
    int i = 0;
    int dataSize = 0;
    // NOTE: as ops on different rows don't affect each other we don't care about executing them in different order
    for (Map.Entry<byte[], Map<byte[], byte[]>> ops4key : opsByKey.entrySet()) {
      keys[i] = ops4key.getKey();
      // todo: should we multiply by column count? For HBase we should.. but may be not for all OVCT implementations
      //       consider returning datasize from table's method that performs ops?
      dataSize += keys[i].length;
      cols[i] = new byte[ops4key.getValue().size()][];
      vals[i] = new byte[cols[i].length][];
      int k = 0;
      for (Map.Entry<byte[], byte[]> colVal : ops4key.getValue().entrySet()) {
        cols[i][k] = colVal.getKey();
        vals[i][k] = colVal.getValue();
        dataSize += cols[i][k].length;
        dataSize += vals[i][k].length;
        k++;
      }
      // todo: consider returning ops that failed (e.g. rows set from the OVCT implementation)
      undos.add(new UndoWrite(tableName, keys[i], cols[i]));
      i++;
    }
    table.put(keys, cols, transaction.getWriteVersion(), vals);
    end(REQ_TYPE_WRITE_LATENCY, begin, writes.size());
    dataSetMetric_write(writes.get(0).getMetricName(), dataSize);

    return new WriteTransactionResult(undos);
  }

  WriteTransactionResult write(OperationContext context, Delete delete,
      Transaction transaction) throws OperationException {
    initialize();
    incMetric(REQ_TYPE_DELETE_NUM_OPS);
    long begin = begin();
    OrderedVersionedColumnarTable table =
        this.findRandomTable(context, delete.getTable());
    table.deleteAll(delete.getKey(), delete.getColumns(), transaction.getWriteVersion());
    end(REQ_TYPE_DELETE_LATENCY, begin);
    dataSetMetric_write(delete.getMetricName(), delete.getSize());
    return new WriteTransactionResult(
        new UndoDelete(delete.getTable(), delete.getKey(), delete.getColumns()));
  }

  WriteTransactionResult write(OperationContext context, Increment increment,
      Transaction transaction) throws OperationException {
    initialize();
    incMetric(REQ_TYPE_INCREMENT_NUM_OPS);
    long begin = begin();
    Map<byte[], Long> map;
    try {
      OrderedVersionedColumnarTable table =
          this.findRandomTable(context, increment.getTable());
      map = table.increment(increment.getKey(), increment.getColumns(), increment.getAmounts(),
                            transaction.getReadPointer(), transaction.getWriteVersion());
    } catch (OperationException e) {
      return new WriteTransactionResult(e.getStatus(), e.getMessage());
    }
    end(REQ_TYPE_INCREMENT_LATENCY, begin);
    dataSetMetric_write(increment.getMetricName(), increment.getSize());
    return new WriteTransactionResult(
        new UndoWrite(increment.getTable(), increment.getKey(), increment.getColumns()), map);
  }

  WriteTransactionResult write(OperationContext context, CompareAndSwap write,
      Transaction transaction) throws OperationException {
    initialize();
    incMetric(REQ_TYPE_COMPARE_AND_SWAP_NUM_OPS);
    long begin = begin();
    try {
      OrderedVersionedColumnarTable table =
          this.findRandomTable(context, write.getTable());
      table.compareAndSwap(write.getKey(), write.getColumn(), write.getExpectedValue(), write.getNewValue(),
                           transaction.getReadPointer(), transaction.getWriteVersion());
    } catch (OperationException e) {
      return new WriteTransactionResult(e.getStatus(), e.getMessage());
    }
    end(REQ_TYPE_COMPARE_AND_SWAP_LATENCY, begin);
    dataSetMetric_write(write.getMetricName(), write.getSize());
    return new WriteTransactionResult(
        new UndoWrite(write.getTable(), write.getKey(), new byte[][] { write.getColumn() }));
  }

  // TTQueues

  /**
   * Enqueue operations always succeed but can be rolled back.
   *
   * They are rolled back with an invalidate.
   */
  WriteTransactionResult write(QueueEnqueue enqueue, Transaction transaction) throws OperationException {
    initialize();
    incMetric(REQ_TYPE_QUEUE_ENQUEUE_NUM_OPS);
    long begin = begin();
    EnqueueResult result = getQueueTable(enqueue.getKey()).enqueue(enqueue.getKey(), enqueue.getEntries(), transaction);
    end(REQ_TYPE_QUEUE_ENQUEUE_LATENCY, begin);
    return new WriteTransactionResult(
        new QueueUndo.QueueUnenqueue(enqueue.getKey(), enqueue.getEntries(), enqueue.getProducer(),
                                     result.getEntryPointers()));
  }

  WriteTransactionResult write(final QueueAck ack, final Transaction transaction)
    throws  OperationException {

    initialize();
    incMetric(REQ_TYPE_QUEUE_ACK_NUM_OPS);
    long begin = begin();
    try {
      queueStateProxy.run(ack.getKey(), ack.getConsumer(),
                          new QueueRunnable() {
                            @Override
                            public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                              getQueueTable(ack.getKey()).ack(ack.getKey(), ack.getEntryPointers(),
                                                              statefulQueueConsumer, transaction);
                            }
                          });
    } catch (OperationException e) {
      // Ack failed, roll back transaction
      return new WriteTransactionResult(e.getStatus(), e.getMessage());
    } finally {
      end(REQ_TYPE_QUEUE_ACK_LATENCY, begin);
    }
    return new WriteTransactionResult(
        new QueueUndo.QueueUnack(ack.getKey(), ack.getEntryPointers(),
                                 ack.getConsumer(), ack.getNumGroups()));
  }

  @Override
  public DequeueResult execute(OperationContext context, final QueueDequeue dequeue) throws OperationException {
    initialize();
    incMetric(REQ_TYPE_QUEUE_DEQUEUE_NUM_OPS);
    long begin = begin();
    final TTQueueTable queueTable = getQueueTable(dequeue.getKey());

    DequeueResult result =
      queueStateProxy.call(dequeue.getKey(), dequeue.getConsumer(),
                                    new QueueCallable<DequeueResult>() {
                                      @Override
                                      public DequeueResult call(StatefulQueueConsumer statefulQueueConsumer)
                                        throws OperationException {
                                        return queueTable.dequeue(dequeue.getKey(), statefulQueueConsumer,
                                                                  oracle.getReadPointer());
                                      }
                                    });
    end(REQ_TYPE_QUEUE_DEQUEUE_LATENCY, begin);
    return result;
  }

  @Override
  public long execute(OperationContext context, GetGroupID getGroupId) throws OperationException {
    initialize();
    incMetric(REQ_TYPE_GET_GROUP_ID_NUM_OPS);
    long begin = begin();
    TTQueueTable table = getQueueTable(getGroupId.getQueueName());
    long groupid = table.getGroupID(getGroupId.getQueueName());
    end(REQ_TYPE_GET_GROUP_ID_LATENCY, begin);
    return groupid;
  }

  @Override
  public OperationResult<QueueInfo> execute(OperationContext context, GetQueueInfo getQueueInfo)
                                                       throws OperationException
  {
    initialize();
    incMetric(REQ_TYPE_GET_QUEUE_INFO_NUM_OPS);
    long begin = begin();
    TTQueueTable table = getQueueTable(getQueueInfo.getQueueName());
    QueueInfo queueInfo = table.getQueueInfo(getQueueInfo.getQueueName());
    end(REQ_TYPE_GET_QUEUE_INFO_LATENCY, begin);
    return queueInfo == null ?
        new OperationResult<QueueInfo>(StatusCode.QUEUE_NOT_FOUND) :
        new OperationResult<QueueInfo>(queueInfo);
  }

  @Override
  public void execute(OperationContext context, final QueueConfigure configure)
    throws OperationException {
    initialize();
    incMetric(REQ_TYPE_QUEUE_CONFIGURE_NUM_OPS);
    long begin = begin();
    final TTQueueTable table = getQueueTable(configure.getQueueName());
    int oldConsumerCount = queueStateProxy.call(configure.getQueueName(), configure.getNewConsumer(),
                                                new QueueCallable<Integer>() {
                                                  @Override
                                                  public Integer call(StatefulQueueConsumer statefulQueueConsumer)
                                                    throws OperationException {
                                                    return table.configure(configure.getQueueName(),
                                                                           statefulQueueConsumer,
                                                                           oracle.getReadPointer());
                                                  }
                                                });

    // Delete the cache state of any removed consumers
    for (int i = configure.getNewConsumer().getGroupSize(); i < oldConsumerCount; ++i) {
      queueStateProxy.deleteConsumerState(configure.getQueueName(), configure.getNewConsumer().getGroupId(), i);
    }
    end(REQ_TYPE_QUEUE_CONFIGURE_LATENCY, begin);
  }

  @Override
  public void execute(OperationContext context, final QueueConfigureGroups configure)
    throws OperationException {
    initialize();
    incMetric(REQ_TYPE_QUEUE_CONFIGURE_GRPS_NUM_OPS);
    long begin = begin();
    final TTQueueTable table = getQueueTable(configure.getQueueName());
    List<Long> removedGroups = table.configureGroups(configure.getQueueName(), configure.getGroupIds());

    // Delete the cache state of any removed groups
    for (Long groupId : removedGroups) {
      queueStateProxy.deleteGroupState(configure.getQueueName(), groupId);
    }
    end(REQ_TYPE_QUEUE_CONFIGURE_GRPS_LATENCY, begin);
  }

  @Override
  public void execute(OperationContext context, final QueueDropInflight op)
    throws OperationException {
    initialize();
    incMetric(REQ_TYPE_QUEUE_DROP_INFLIGHT_OPS);
    long begin = begin();
    final TTQueueTable table = getQueueTable(op.getQueueName());
    queueStateProxy.run(op.getQueueName(), op.getConsumer(),
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            table.dropInflightState(op.getQueueName(), op.getConsumer(), oracle.getReadPointer());
                          }
                        });

    end(REQ_TYPE_QUEUE_DROP_INFLIGHT_LATENCY, begin);
  }

  @Override
  public Scanner scan(OperationContext context, @Nullable Transaction transaction, Scan scan)
    throws OperationException {
    initialize();
    incMetric(REQ_TYPE_SCAN_NUM_OPS);
    long begin = begin();
    ReadPointer pointer = getReadPointer(transaction);
    OrderedVersionedColumnarTable table = this.findRandomTable(context, scan.getTable());
    Scanner scanner = table.scan(scan.getStartRow(), scan.getStopRow(), scan.getColumns(), pointer);
    end(REQ_TYPE_SCAN_LATENCY, begin);
    dataSetMetric_read(scan.getMetricName());
    return scanner;
  }

  Transaction startTransaction(boolean trackChanges) {
    incMetric(REQ_TYPE_START_TRANSACTION_NUM_OPS);
    return this.oracle.startTransaction(trackChanges);
  }

  void addToTransaction(Transaction transaction, List<Undo> undos)
      throws OmidTransactionException {
    this.oracle.addToTransaction(transaction, undos);
  }

  TransactionResult commitTransaction(Transaction transaction)
    throws OmidTransactionException {
    incMetric(REQ_TYPE_COMMIT_TRANSACTION_NUM_OPS);
    return this.oracle.commitTransaction(transaction);
  }

  TransactionResult abortTransaction(Transaction transaction)
    throws OmidTransactionException {
    incMetric(REQ_TYPE_COMMIT_TRANSACTION_NUM_OPS);
    return this.oracle.abortTransaction(transaction);
  }


  private void attemptUndo(OperationContext context,
                           Transaction transaction,
                           List<Undo> undos)
      throws OperationException {
    // Perform queue invalidates
    cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_AbortedTransactions", 1);
    for (Undo undo : undos) {
      if (undo instanceof QueueUndo) {
        QueueUndo queueUndo = (QueueUndo) undo;
        queueUndo.execute(queueStateProxy, transaction, getQueueTable(queueUndo.queueName));
      }
      if (undo instanceof UndoWrite) {
        UndoWrite tableUndo = (UndoWrite) undo;
        OrderedVersionedColumnarTable table =
            this.findRandomTable(context, tableUndo.getTable());
        if (tableUndo instanceof UndoDelete) {
          table.undeleteAll(tableUndo.getKey(), tableUndo.getColumns(), transaction.getWriteVersion());
        } else {
          table.delete(tableUndo.getKey(), tableUndo.getColumns(), transaction.getWriteVersion());
        }
      }
    }
    // if any of the undos fails, we won't reach this point.
    // That is, the tx will remain in the oracle as invalid
    oracle.removeTransaction(transaction);
  }

  // Single Write Operations (Wrapped and called in a transaction batch)

  @SuppressWarnings("unused")
  private void unsupported(String msg) {
    throw new RuntimeException(msg);
  }

  @Override
  public void commit(OperationContext context, WriteOperation write) throws OperationException {
    commit(context, Collections.singletonList(write));
  }

  private TTQueueTable getQueueTable(byte[] queueName) {
    if (Bytes.startsWith(queueName, TTQueue.QUEUE_NAME_PREFIX)) {
      return this.queueTable;
    }
    if (Bytes.startsWith(queueName, TTQueue.STREAM_NAME_PREFIX)) {
      return this.streamTable;
    }
    // by default, use queue table
    return this.queueTable;
  }

  /**
   * A utility method that ensures this class is properly initialized before
   * it can be used. This currently entails creating real objects for all
   * our table handlers.
   */
  private synchronized void initialize() throws OperationException {

    if (this.randomTable == null) {
      this.metaTable = this.tableHandle.getTable(Bytes.toBytes("meta"));
      this.randomTable = this.tableHandle.getTable(Bytes.toBytes("random"));
      this.queueTable = this.tableHandle.getQueueTable(Bytes.toBytes("queues"));
      this.streamTable = this.tableHandle.getStreamTable(Bytes.toBytes("streams"));
      this.namedTables = Maps.newConcurrentMap();
      this.metaStore = new SerializingMetaDataStore(this);
    }
  }

  private void validateTransaction(Transaction transaction) throws OmidTransactionException {
    if (talkToOracle) {
      oracle.validateTransaction(transaction);
    } else {
      if (transaction == null) {
        throw new OmidTransactionException(StatusCode.INVALID_TRANSACTION,
                                           "Tx is null. Cannot operate without transaction");
      }
    }
  }

  private ReadPointer getReadPointer(Transaction transaction) throws OmidTransactionException {
    if (transaction != null) {
      return transaction.getReadPointer();
    }

    return this.oracle.getReadPointer();
  }

} // end of OmitTransactionalOperationExecutor
