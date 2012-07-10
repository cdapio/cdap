package com.continuuity.data.operation.executor.simple;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.FormatFabric;
import com.continuuity.data.operation.OrderedWrite;
import com.continuuity.data.operation.ReadModifyWrite;
import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.*;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetGroupID;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetQueueMeta;
import com.continuuity.data.operation.ttqueue.QueueAdmin.QueueMeta;
import com.continuuity.data.table.ColumnarTable;
import com.continuuity.data.table.ColumnarTableHandle;
import com.continuuity.data.table.OVCTableHandle;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SimpleOperationExecutor implements OperationExecutor {

  final ColumnarTableHandle tableHandle;

  final ColumnarTable randomTable;

  final ColumnarTable orderedTable;

  final TTQueueTable queueTable;

  public SimpleOperationExecutor(ColumnarTableHandle tableHandle) {
    this.tableHandle = tableHandle;
    this.randomTable = tableHandle.getTable(Bytes.toBytes("random"));
    this.orderedTable = tableHandle.getTable(Bytes.toBytes("ordered"));
    this.queueTable = tableHandle.getQueueTable(Bytes.toBytes("queues"));
  }

  // Batch of Writes

  /**
   * Performs the specified writes synchronously and in sequence.
   *
   * If an error is reached, execution of subsequent operations is skipped and
   * false is returned.  If all operations are performed successfully, returns
   * true.
   *
   * @param writes write operations to be performed in sequence
   * @return true if all operations succeeded, false if not
   */
  @Override
  public BatchOperationResult execute(List<WriteOperation> writes) {
    for (WriteOperation write : writes) {
      if(!execute(write)) return new BatchOperationResult(false,
          "Write operation failed");
    }
    return new BatchOperationResult(true);
  }

  private boolean execute(WriteOperation write) {
    if (write instanceof Write) {
      if (!execute((Write)write)) return false;
    } else if (write instanceof OrderedWrite) {
      if (!execute((OrderedWrite)write)) return false;
    } else if (write instanceof ReadModifyWrite) {
      if (!execute((ReadModifyWrite)write)) return false;
    } else if (write instanceof Increment) {
      if (!execute((Increment)write)) return false;
    } else if (write instanceof CompareAndSwap) {
      if (!execute((CompareAndSwap)write)) return false;
    }
    return true;
  }

  // Static Constants

  static final byte [] COLUMN = Bytes.toBytes("c");

  static final int MAX_RETRIES = 10;

  // Single Writes

  @Override
  public boolean execute(Write write) {
    this.randomTable.put(write.getKey(), write.getColumns(), write.getValues());
    return true;
  }

  @Override
  public boolean execute(Delete delete) {
    this.randomTable.delete(delete.getKey(), delete.getColumns()[0]);
    return true;
  }

  // Conditional Writes

  @Override
  public boolean execute(CompareAndSwap cas) {
    return this.randomTable.compareAndSwap(cas.getKey(), COLUMN,
        cas.getExpectedValue(), cas.getNewValue());
  }

  // Value Returning Read-Modify-Writes

  @Override
  public boolean execute(Increment inc) {
    Long amount =
        this.randomTable.increment(inc.getKey(), inc.getColumns()[0],
            inc.getAmounts()[0]);
    Map<byte[],Long> map = new TreeMap<byte[],Long>(Bytes.BYTES_COMPARATOR);
    map.put(Operation.KV_COL, amount);
    return true;
  }


  // Simple Reads

  @Override
  public byte[] execute(ReadKey read) {
    return this.randomTable.get(read.getKey(), Operation.KV_COL);
  }

  @Override
  public DequeueResult execute(QueueDequeue dequeue) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long execute(GetGroupID getGroupId) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean execute(QueueAck ack) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(QueueEnqueue enqueue) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public OVCTableHandle getTableHandle() {
    return null;
  }

  @Override
  public List<byte[]> execute(ReadAllKeys readKeys) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<byte[], byte[]> execute(Read read) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<byte[], byte[]> execute(ReadColumnRange readColumnRange) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueueMeta execute(GetQueueMeta getQueueMeta) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void execute(FormatFabric formatFabric) {
    // TODO Auto-generated method stub
    
  }
}
