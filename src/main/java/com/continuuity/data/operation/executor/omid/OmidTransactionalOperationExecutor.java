/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid;

import com.continuuity.api.data.*;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.*;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.TransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.QueueInvalidate.QueueFinalize;
import com.continuuity.data.operation.executor.omid.QueueInvalidate.QueueUnack;
import com.continuuity.data.operation.executor.omid.QueueInvalidate.QueueUnenqueue;
import com.continuuity.data.operation.executor.omid.memory.MemoryRowSet;
import com.continuuity.data.operation.ttqueue.*;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetGroupID;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.ReadPointer;
import com.continuuity.data.util.TupleMetaDataAnnotator.DequeuePayload;
import com.continuuity.data.util.TupleMetaDataAnnotator.EnqueuePayload;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.continuuity.data.operation.ttqueue.QueueAdmin.GetQueueInfo;
import static com.continuuity.data.operation.ttqueue.QueueAdmin.QueueInfo;

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
  @Inject
  TransactionOracle oracle;

  /**
   * The {@link OVCTableHandle} handle used to get references to tables.
   */
  @Inject
  OVCTableHandle tableHandle;

  private OrderedVersionedColumnarTable metaTable;
  private OrderedVersionedColumnarTable randomTable;

  private MetaDataStore metaStore;

  private TTQueueTable queueTable;
  private TTQueueTable streamTable;

  public static boolean DISABLE_QUEUE_PAYLOADS = false;

  static int MAX_DEQUEUE_RETRIES = 200;
  static long DEQUEUE_RETRY_SLEEP = 5;
  static final byte DATA = (byte)0x00; // regular data
  static final byte DELETE_VERSION = (byte)0x01; // delete of a specific version
  static final byte DELETE_ALL = (byte)0x02; // delete of all versions of a cell up to specific version

  // Metrics

  /* -------------------  data fabric system metrics ---------------- */
  private CMetrics cmetric = new CMetrics(MetricType.System);

  private static final String METRIC_PREFIX = "omid-opex-";

  private void requestMetric(String requestType) {
    // TODO rework metric emission to avoid always generating the metric names
    cmetric.meter(METRIC_PREFIX + requestType + "-numops", 1);
  }

  private long begin() { return System.currentTimeMillis(); }
  private void end(String requestType, long beginning) {
    cmetric.histogram(METRIC_PREFIX + requestType + "-latency",
        System.currentTimeMillis() - beginning);
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

  private int streamSizeEstimate(byte[] streamName, byte[] data) {
    // assume HBase uses space for the stream name, the data, and some metadata
    return streamName.length + data.length + 50;
  }

  private void streamMetric(byte[] streamName, byte[] data) {
    ImmutablePair<String, String> names = getStreamMetricNames(streamName);
    streamMetric.meter(names.getFirst(), 1);
    streamMetric.meter(names.getSecond(), streamSizeEstimate(streamName, data));
  }

  private void namedTableMetric_read(String tableName) {
    streamMetric.meter("com.continuuity.data.dataset.read." + tableName, 1);
  }
  
  private void namedTableMetric_write(String tableName, int dataSize) {
    streamMetric.meter("com.continuuity.data.dataset.write." + tableName, 1);
    streamMetric.meter("com.continuuity.data.dataset.storage." + tableName, dataSize);
  }
  
  /* -------------------  end metrics ---------------- */

  // named table management

  // a map of logical table name to existing <real name, table>, caches
  // the meta data store and the ovc table handle
  // there are three possible states for a table:
  // 1. table does not exist or is not known -> no entry
  // 2. table is being created -> entry with real name, but null for the table
  // 3. table is known -> entry with name and table
  ConcurrentMap<ImmutablePair<String,String>,
      ImmutablePair<byte[], OrderedVersionedColumnarTable>> namedTables;

  // method to find - and if necessary create - a table
  OrderedVersionedColumnarTable findRandomTable(OperationContext context, String name) throws OperationException {

    // check whether it is one of the default tables these are always
    // pre-loaded at initializaton and we can just return them
    if (null == name)
      return this.randomTable;
    if ("meta".equals(name))
      return this.metaTable;

    // look up table in in-memory map. if this returns:
    // an actual name and OVCTable, return that OVCTable
    ImmutablePair<String, String> tableKey = new
        ImmutablePair<String, String>(context.getAccount(), name);
    ImmutablePair<byte[], OrderedVersionedColumnarTable> nameAndTable =
        this.namedTables.get(tableKey);
    if (nameAndTable != null) {
      if (nameAndTable.getSecond() != null)
        return nameAndTable.getSecond();

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
          return waitForTableToMaterializeInMeta(context, name, meta);
        }
        else throw e;
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

  private OrderedVersionedColumnarTable waitForTableToMaterialize(
      ImmutablePair<String, String> tableKey) {
    while (true) {
      ImmutablePair<byte[], OrderedVersionedColumnarTable> nameAndTable =
          this.namedTables.get(tableKey);
      if (nameAndTable == null) {
        throw new InternalError("In-memory entry went from non-null to null " +
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

  OrderedVersionedColumnarTable waitForTableToMaterializeInMeta(
      OperationContext context, String name, MetaDataEntry meta)
    throws OperationException {

    while(true) {
      // If this returns: An actual name and status Ready: The table is ready
      // to use, open the table, add it to MEM and return it
      if ("ready".equals(meta.getTextField("status"))) {
        byte[] actualName = meta.getBinaryField("actual");
        if (actualName == null)
          throw new InternalError("Encountered meta data entry of type " +
              "\"namedTable\" without actual name for table name \"" +
              name +"\".");
        OrderedVersionedColumnarTable table =
            getTableHandle().getTable(actualName);
        if (table == null)
          throw new InternalError("table handle \"" + getTableHandle()
              .getName() + "\": getTable returned null for actual table name "
              + "\"" + new String(actualName) + "\"");

        // update MEM. This can be ordinary put, because even if some other
        // thread updated it in the meantime, it would have put the same table.
        ImmutablePair<String, String> tableKey = new
            ImmutablePair<String, String>(context.getAccount(), name);
        this.namedTables.put(tableKey,
            new ImmutablePair<byte[], OrderedVersionedColumnarTable>(
                actualName, table));
        return table;
      }
      // An actual name and status Pending: The table is being created. Loop
      // and repeat MDS read until status is Ready and see previous case
      else if (!"pending".equals(meta.getTextField("status"))) {
        throw new InternalError("Found meta data entry with unkown status " +
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
      if (meta == null)
        throw new InternalError("Meta data entry went from non-null to null " +
            "for table \"" + name + "\"");

      // TODO should this time out after some time or number of attempts?
    }
  }

  // Single reads

  @Override
  public OperationResult<byte[]> execute(OperationContext context,
                                         ReadKey read)
      throws OperationException {
    initialize();
    requestMetric("ReadKey");
    long begin = begin();
    OperationResult<byte[]> result =
        read(context, read, this.oracle.getReadPointer());
    end("ReadKey", begin);
    namedTableMetric_read(read.getTable());
    return result;
  }

  OperationResult<byte[]> read(OperationContext context,
                               ReadKey read, ReadPointer pointer)
      throws OperationException {
    OrderedVersionedColumnarTable table =
        this.findRandomTable(context, read.getTable());
    return table.get(read.getKey(), Operation.KV_COL, pointer);
  }

  @Override
  public OperationResult<List<byte[]>> execute(OperationContext context,
                                               ReadAllKeys readKeys)
      throws OperationException {
    initialize();
    requestMetric("ReadAllKeys");
    long begin = begin();
    OrderedVersionedColumnarTable table = this.findRandomTable(context, readKeys.getTable());
    List<byte[]> result = table.getKeys(readKeys.getLimit(), readKeys.getOffset(), this.oracle.getReadPointer());
    end("ReadKey", begin);
    namedTableMetric_read(readKeys.getTable());
    return new OperationResult<List<byte[]>>(result);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context, Read read)
      throws OperationException {
    initialize();
    requestMetric("Read");
    long begin = begin();
    OrderedVersionedColumnarTable table = this.findRandomTable(context, read.getTable());
    OperationResult<Map<byte[], byte[]>> result =
      table.get(read.getKey(), read.getColumns(), this.oracle.getReadPointer());
    end("Read", begin);
    namedTableMetric_read(read.getTable());
    return result;
  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  execute(OperationContext context,
          ReadColumnRange readColumnRange) throws OperationException {
    initialize();
    requestMetric("ReadColumnRange");
    long begin = begin();
    OrderedVersionedColumnarTable table =
        this.findRandomTable(context, readColumnRange.getTable());
    OperationResult<Map<byte[], byte[]>> result = table.get(
        readColumnRange.getKey(), readColumnRange.getStartColumn(),
        readColumnRange.getStopColumn(), readColumnRange.getLimit(),
        this.oracle.getReadPointer());
    end("ReadColumnRange", begin);
    namedTableMetric_read(readColumnRange.getTable());
    return result;
  }

  // Administrative calls

  @Override
  public void execute(OperationContext context,
                      ClearFabric clearFabric) throws OperationException {
    initialize();
    requestMetric("ClearFabric");
    long begin = begin();
    if (clearFabric.shouldClearData()) this.randomTable.clear();
    if (clearFabric.shouldClearTables()) {
      List<MetaDataEntry> entries = this.metaStore.list(
          context, context.getAccount(), null, "namedTable", null);
      for (MetaDataEntry entry : entries) {
        String name = entry.getId();
        OrderedVersionedColumnarTable table = findRandomTable(context, name);
        table.clear();
        this.namedTables.remove(new ImmutablePair<String,
            String>(context.getAccount(),name));
        this.metaStore.delete(context, entry.getAccount(),
            entry.getApplication(), entry.getType(), entry.getId());
      }
    }
    if (clearFabric.shouldClearMeta()) this.metaTable.clear();
    if (clearFabric.shouldClearQueues()) this.queueTable.clear();
    if (clearFabric.shouldClearStreams()) this.streamTable.clear();
    end("ClearFabric", begin);
  }

  @Override
  public void execute(OperationContext context, OpenTable openTable)
      throws OperationException {
    initialize();
    findRandomTable(context, openTable.getTableName());
  }

  // Write batches

  @Override
  public void execute(OperationContext context,
                      List<WriteOperation> writes)
      throws OperationException {
    initialize();
    requestMetric("WriteOperationBatch");
    long begin = begin();
    cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_NumReqs", writes.size());
    execute(context, writes, startTransaction());
    end("WriteOperationBatch", begin);
  }

  private void executeAsBatch(OperationContext context,
                              WriteOperation write)
      throws OperationException {
    execute(context, Collections.singletonList(write));
  }

  void execute(OperationContext context, List<WriteOperation> writes,
               ImmutablePair<ReadPointer,Long> pointer)
      throws OperationException {

    if (writes.isEmpty()) return;

    // Re-order operations (create a copy for now)
    List<WriteOperation> orderedWrites = new ArrayList<WriteOperation>(writes);
    Collections.sort(orderedWrites, new WriteOperationComparator());

    // Execute operations
    RowSet rows = new MemoryRowSet();
    List<Delete> deletes = new ArrayList<Delete>(writes.size());
    List<QueueInvalidate> invalidates = new ArrayList<QueueInvalidate>();

    // Track a map from increment operation ids to post-increment values
    Map<Long,Long> incrementResults = new TreeMap<Long,Long>();

    for (WriteOperation write : orderedWrites) {

      // See if write operation is an enqueue, and if so, update serialized data
      if (write instanceof QueueEnqueue) {
        processEnqueue((QueueEnqueue)write, incrementResults);
      }
      WriteTransactionResult writeTxReturn = dispatchWrite(context, write, pointer);

      if (!writeTxReturn.success) {
        // Write operation failed
        cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_FailedWrites", 1);
        abortTransaction(context, pointer, deletes, invalidates);
        throw new OmidTransactionException(
            writeTxReturn.statusCode, writeTxReturn.message);
      } else {
        // Write was successful.  Store delete if we need to abort and continue
        deletes.addAll(writeTxReturn.deletes);
        if (writeTxReturn.invalidate != null) {
          // Queue operation
          invalidates.add(writeTxReturn.invalidate);
        } else {
          // Normal write operation
          rows.addRow(write.getKey());
        }
        // See if write operation was an Increment, and if so, add result to map
        if (write instanceof Increment) {
          incrementResults.put(write.getId(), writeTxReturn.incrementValue);
        }
      }
    }

    // All operations completed successfully, commit transaction
    if (!commitTransaction(pointer, rows)) {
      // Commit failed, abort
      cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_FailedCommits", 1);
      abortTransaction(context, pointer, deletes, invalidates);
      throw new OmidTransactionException(StatusCode.WRITE_CONFLICT,
          "Commit of transaction failed, transaction aborted");
    }

    // If last operation was an ack, finalize it
    if (orderedWrites.get(orderedWrites.size() - 1) instanceof QueueAck) {
      QueueAck ack = (QueueAck)orderedWrites.get(orderedWrites.size() - 1);
      QueueFinalize finalize = new QueueFinalize(ack.getKey(),
          ack.getEntryPointer(), ack.getConsumer(), ack.getNumGroups());
      finalize.execute(getQueueTable(ack.getKey()), pointer);
    }

    // Transaction was successfully committed, emit metrics
    // - for the transaction
    cmetric.meter(
        METRIC_PREFIX + "WriteOperationBatch_SuccessfulTransactions", 1);
    // for each queue operation (enqueue or ack)
    for (QueueInvalidate invalidate : invalidates)
      if (invalidate instanceof QueueUnenqueue) {
        QueueUnenqueue unenqueue = (QueueUnenqueue)invalidate;
        QueueProducer producer = unenqueue.producer;
        enqueueMetric(unenqueue.queueName, producer);
        if (isStream(unenqueue.queueName))
          streamMetric(unenqueue.queueName, unenqueue.data);
      } else if (invalidate instanceof QueueUnack) {
        QueueUnack unack = (QueueUnack)invalidate;
        QueueConsumer consumer = unack.consumer;
        ackMetric(unack.queueName, consumer);
      }
  }

  /**
   * Deserializes enqueue data into enqueue payload, checks if any fields are
   * marked to contain increment values, construct a dequeue payload, update any
   * fields necessary, and finally update the enqueue data to contain a dequeue
   * payload.
   */
  private void processEnqueue(QueueEnqueue enqueue,
      Map<Long, Long> incrementResults) throws OperationException {
    if (DISABLE_QUEUE_PAYLOADS) return;
    // Deserialize enqueue payload
    byte [] enqueuePayloadBytes = enqueue.getData();
    EnqueuePayload enqueuePayload;
    try {
      enqueuePayload = EnqueuePayload.read(enqueuePayloadBytes);
    } catch (IOException e) {
      // Unable to deserialize the enqueue payload, fatal error (if queues are
      // not using payloads, change DISABLE_QUEUE_PAYLOADS=true
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    Map<String,Long> fieldsToIds = enqueuePayload.getOperationIds();
    Map<String,Long> fieldsToValues = new TreeMap<String,Long>();

    // For every field-to-id mapping, find increment result
    for (Map.Entry<String,Long> fieldAndId : fieldsToIds.entrySet()) {
      String field = fieldAndId.getKey();
      Long operationId = fieldAndId.getValue();
      Long incrementValue = incrementResults.get(operationId);
      if (incrementValue == null) {
        throw new OperationException(StatusCode.INTERNAL_ERROR,
            "Field specified as containing an increment result but no " +
                "matching increment operation found");
      }
      // Store field-to-value in map for dequeue payload
      fieldsToValues.put(field, incrementValue);
    }

    // Serialize dequeue payload and overwrite enqueue data
    try {
      enqueue.setData(DequeuePayload.write(fieldsToValues,
          enqueuePayload.getSerializedTuple()));
    } catch (IOException e) {
      // Fatal error serializing dequeue payload
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public OVCTableHandle getTableHandle() {
    return this.tableHandle;
  }

  static final List<Delete> noDeletes = Collections.emptyList();

  private class WriteTransactionResult {
    final boolean success;
    final int statusCode;
    final String message;
    final List<Delete> deletes;
    final QueueInvalidate invalidate;
    Long incrementValue;

    WriteTransactionResult(boolean success, int status, String message,
                           List<Delete> deletes, QueueInvalidate invalidate) {
      this.success = success;
      this.statusCode = status;
      this.message = message;
      this.deletes = deletes;
      this.invalidate = invalidate;
    }

    // successful, one delete to undo
    WriteTransactionResult(Delete delete) {
      this(true, StatusCode.OK, null, Collections.singletonList(delete), null);
    }

    // successful increment, one delete to undo
    WriteTransactionResult(Delete delete, Long incrementValue) {
      this(true, StatusCode.OK, null, Collections.singletonList(delete), null);
      this.incrementValue = incrementValue;
    }

    // successful, one queue operation to invalidate
    WriteTransactionResult(QueueInvalidate invalidate) {
      this(true, StatusCode.OK, null, noDeletes, invalidate);
    }

    // failure with status code and message, nothing to undo
    WriteTransactionResult(int status, String message) {
      this(false, status, message, noDeletes, null);
    }
  }

  /**
   * Actually perform the various write operations.
   */
  private WriteTransactionResult dispatchWrite(
      OperationContext context, WriteOperation write,
      ImmutablePair<ReadPointer,Long> pointer) throws OperationException {
    if (write instanceof Write) {
      return write(context, (Write)write, pointer);
    } else if (write instanceof Delete) {
      return write(context, (Delete)write, pointer);
    } else if (write instanceof Increment) {
      return write(context, (Increment)write, pointer);
    } else if (write instanceof CompareAndSwap) {
      return write(context, (CompareAndSwap)write, pointer);
    } else if (write instanceof QueueEnqueue) {
      return write((QueueEnqueue)write, pointer);
    } else if (write instanceof QueueAck) {
      return write((QueueAck)write, pointer);
    }
    return new WriteTransactionResult(StatusCode.INTERNAL_ERROR,
        "Unknown write operation " + write.getClass().getName());
  }

  WriteTransactionResult write(OperationContext context, Write write, ImmutablePair<ReadPointer,Long> pointer)
    throws OperationException {
    initialize();
    requestMetric("Write");
    long begin = begin();
    OrderedVersionedColumnarTable table = this.findRandomTable(context, write.getTable());
    table.put(write.getKey(), write.getColumns(), pointer.getSecond(), write.getValues());
    end("Write", begin);
    namedTableMetric_write(write.getTable(), write.getSize());
//    return new WriteTransactionResult(new Delete(write.getTable(), write.getKey(), write.getColumns()));
    return new WriteTransactionResult(new Delete(write.getTable(), write.getKey(), write.getColumns()));
  }

  WriteTransactionResult write(OperationContext context, Delete delete,
      ImmutablePair<ReadPointer, Long> pointer) throws OperationException {
    initialize();
    requestMetric("Delete");
    long begin = begin();
    OrderedVersionedColumnarTable table =
        this.findRandomTable(context, delete.getTable());
    table.deleteAll(delete.getKey(), delete.getColumns(),
        pointer.getSecond());
    end("Delete", begin);
    namedTableMetric_write(delete.getTable(), delete.getSize());
    return new WriteTransactionResult(
        new Undelete(delete.getTable(), delete.getKey(), delete.getColumns()));
  }

  WriteTransactionResult write(OperationContext context, Increment increment,
      ImmutablePair<ReadPointer,Long> pointer) throws OperationException {
    initialize();
    requestMetric("Increment");
    long begin = begin();
    Long incrementValue;
    try {
      @SuppressWarnings("unused")
      OrderedVersionedColumnarTable table =
          this.findRandomTable(context, increment.getTable());
      Map<byte[],Long> map =
          table.increment(increment.getKey(),
              increment.getColumns(), increment.getAmounts(),
              pointer.getFirst(), pointer.getSecond());
      incrementValue = map.values().iterator().next();
    } catch (OperationException e) {
      return new WriteTransactionResult(e.getStatus(), e.getMessage());
    }
    end("Increment", begin);
    namedTableMetric_write(increment.getTable(), increment.getSize());
    return new WriteTransactionResult(
        new Delete(increment.getTable(), increment.getKey(),
            increment.getColumns()), incrementValue);
  }

  WriteTransactionResult write(OperationContext context, CompareAndSwap write,
      ImmutablePair<ReadPointer,Long> pointer) throws OperationException {
    initialize();
    requestMetric("CompareAndSwap");
    long begin = begin();
    try {
      OrderedVersionedColumnarTable table =
          this.findRandomTable(context, write.getTable());
      table.compareAndSwap(write.getKey(),
          write.getColumn(), write.getExpectedValue(), write.getNewValue(),
          pointer.getFirst(), pointer.getSecond());
    } catch (OperationException e) {
      return new WriteTransactionResult(e.getStatus(), e.getMessage());
    }
    end("CompareAndSwap", begin);
    namedTableMetric_write(write.getTable(), write.getSize());
    return new WriteTransactionResult(
        new Delete(write.getTable(), write.getKey(), write.getColumn()));
  }

  // TTQueues

  /**
   * EnqueuePayload operations always succeed but can be rolled back.
   *
   * They are rolled back with an invalidate.
   */
  WriteTransactionResult write(QueueEnqueue enqueue,
      ImmutablePair<ReadPointer, Long> pointer) throws OperationException {
    initialize();
    requestMetric("QueueEnqueue");
    long begin = begin();
    EnqueueResult result = getQueueTable(enqueue.getKey()).enqueue(
        enqueue.getKey(), enqueue.getData(), pointer.getSecond());
    end("QueueEnqueue", begin);
    return new WriteTransactionResult(
        new QueueUnenqueue(enqueue.getKey(), enqueue.getData(),
            enqueue.getProducer(), result.getEntryPointer()));
  }

  WriteTransactionResult write(QueueAck ack,
      @SuppressWarnings("unused") ImmutablePair<ReadPointer, Long> pointer)
      throws OperationException {

    initialize();
    requestMetric("QueueAck");
    long begin = begin();
    try {
      getQueueTable(ack.getKey()).ack(ack.getKey(),
          ack.getEntryPointer(), ack.getConsumer());
    } catch (OperationException e) {
      // Ack failed, roll back transaction
      return new WriteTransactionResult(StatusCode.ILLEGAL_ACK,
          "Attempt to ack a dequeue of a different consumer");
    } finally {
      end("QueueAck", begin);
    }
    return new WriteTransactionResult(
        new QueueUnack(ack.getKey(), ack.getEntryPointer(), ack.getConsumer()));
  }

  @Override
  public DequeueResult execute(OperationContext context,
                               QueueDequeue dequeue)
      throws OperationException {
    initialize();
    requestMetric("QueueDequeue");
    long begin = begin();
    int retries = 0;
    long start = System.currentTimeMillis();
    TTQueueTable queueTable = getQueueTable(dequeue.getKey());
    while (retries < MAX_DEQUEUE_RETRIES) {
      DequeueResult result = queueTable.dequeue(dequeue.getKey(),
          dequeue.getConsumer(), dequeue.getConfig(),
          this.oracle.getReadPointer());
      if (result.shouldRetry()) {
        retries++;
        try {
          if (DEQUEUE_RETRY_SLEEP > 0) Thread.sleep(DEQUEUE_RETRY_SLEEP);
        } catch (InterruptedException e) {
          e.printStackTrace();
          // continue in loop
        }
        continue;
      }
      end("QueueDequeue", begin);
      return result;
    }
    long end = System.currentTimeMillis();
    end("QueueDequeue", begin);
    throw new OperationException(StatusCode.TOO_MANY_RETRIES,
        "Maximum retries (retried " + retries + " times over " + (end-start) +
        " millis");
  }

  @Override
  public long execute(OperationContext context,
                      GetGroupID getGroupId)
      throws OperationException {
    initialize();
    requestMetric("GetGroupID");
    long begin = begin();
    TTQueueTable table = getQueueTable(getGroupId.getQueueName());
    long groupid = table.getGroupID(getGroupId.getQueueName());
    end("GetGroupID", begin);
    return groupid;
  }

  @Override
  public OperationResult<QueueAdmin.QueueInfo>
  execute(OperationContext context, GetQueueInfo getQueueInfo)
      throws OperationException
  {
    initialize();
    requestMetric("GetQueueInfo");
    long begin = begin();
    TTQueueTable table = getQueueTable(getQueueInfo.getQueueName());
    QueueInfo queueInfo = table.getQueueInfo(getQueueInfo.getQueueName());
    end("GetQueueInfo", begin);
    return queueInfo == null ?
        new OperationResult<QueueInfo>(StatusCode.QUEUE_NOT_FOUND) :
        new OperationResult<QueueAdmin.QueueInfo>(queueInfo);
  }

  ImmutablePair<ReadPointer, Long> startTransaction() {
    requestMetric("StartTransaction");
    return this.oracle.getNewPointer();
  }

  boolean commitTransaction(ImmutablePair<ReadPointer, Long> pointer,
      RowSet rows) throws OmidTransactionException {
    requestMetric("CommitTransaction");
    return this.oracle.commit(pointer.getSecond(), rows);
  }

  private void abortTransaction(OperationContext context,
                                ImmutablePair<ReadPointer, Long> pointer,
                                List<Delete> deletes,
                                List<QueueInvalidate> invalidates)
      throws OperationException {
    // Perform queue invalidates
    cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_AbortedTransactions", 1);
    for (QueueInvalidate invalidate : invalidates) {
      invalidate.execute(getQueueTable(invalidate.queueName), pointer);
    }
    // Perform deletes
    for (Delete delete : deletes) {
      assert(delete != null);
      OrderedVersionedColumnarTable table =
          this.findRandomTable(context, delete.getTable());
      if (delete instanceof Undelete) {
        table.undeleteAll(delete.getKey(), delete.getColumns(),
            pointer.getSecond());
      } else {
        table.delete(delete.getKey(), delete.getColumns(),
            pointer.getSecond());
      }
    }
    // Notify oracle
    this.oracle.aborted(pointer.getSecond());
  }

  // Single Write Operations (Wrapped and called in a transaction batch)

  @SuppressWarnings("unused")
  private void unsupported(String msg) {
    throw new RuntimeException(msg);
  }

  @Override
  public void execute(OperationContext context,
                      Write write) throws OperationException {
    executeAsBatch(context, write);
  }

  @Override
  public void execute(OperationContext context,
                      Delete delete) throws OperationException {
    executeAsBatch(context, delete);
  }

  @Override
  public void execute(OperationContext context,
                      Increment inc) throws OperationException {
    executeAsBatch(context, inc);
  }

  @Override
  public void execute(OperationContext context,
                      CompareAndSwap cas) throws OperationException {
    executeAsBatch(context, cas);
  }

  @Override
  public void execute(OperationContext context,
                      QueueAck ack) throws OperationException {
    executeAsBatch(context, ack);
  }

  @Override
  public void execute(OperationContext context,
                      QueueEnqueue enqueue) throws OperationException {
    executeAsBatch(context, enqueue);
  }

  private TTQueueTable getQueueTable(byte[] queueName) {
    if (Bytes.startsWith(queueName, TTQueue.QUEUE_NAME_PREFIX))
      return this.queueTable;
    if (Bytes.startsWith(queueName, TTQueue.STREAM_NAME_PREFIX))
      return this.streamTable;
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

} // end of OmitTransactionalOperationExecutor
