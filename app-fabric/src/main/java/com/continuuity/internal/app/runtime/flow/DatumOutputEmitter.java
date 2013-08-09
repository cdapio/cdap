package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.internal.app.runtime.OutputSubmitter;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.internal.io.Schema;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @param <T>
 */
public final class DatumOutputEmitter<T> implements OutputEmitter<T>, OutputSubmitter {

  public static final Function<Object, Integer> PARTITION_MAP_TRANSFORMER = new PartitionMapTransformer();
  public final Function<DataObject<T>, QueueEntry> queueEntryGenerator = new DataObjectToQueueEntry();

  private final BasicFlowletContext flowletContext;
  private final QueueProducer queueProducer;
  private final QueueName queueName;
  private final byte[] schemaHash;
  private final DatumWriter<T> writer;
  private final BlockingQueue<DataObject<T>> dataQueue;

  public DatumOutputEmitter(BasicFlowletContext flowletContext,
                            QueueProducer queueProducer,
                            QueueName queueName,
                            Schema schema,
                            DatumWriter<T> writer) {
    this.flowletContext = flowletContext;
    this.queueProducer = queueProducer;
    this.queueName = queueName;
    this.schemaHash = schema.getSchemaHash().toByteArray();
    this.writer = writer;
    this.dataQueue = new LinkedBlockingQueue<DataObject<T>>();
  }

  @Override
  public void emit(T data) {
    emit(new DataObject<T>(data));
  }

  @Override
  public void emit(T data, String partitionKey, Object partitionValue) {
    emit(new DataObject<T>(data, partitionKey, partitionValue));
  }

  @Override
  public void emit(T data, Map<String, Object> partitions) {
    emit(new DataObject<T>(data, partitions));
  }

  private void emit(DataObject<T> dataObject) {
    dataQueue.add(dataObject);
  }

  @Override
  public void submit(TransactionAgent agent) throws OperationException {
    List<DataObject<T>> outputs = Lists.newArrayListWithExpectedSize(dataQueue.size());
    dataQueue.drainTo(outputs);
    flowletContext.getSystemMetrics().gauge("process.events.outs." + queueName.getSimpleName(), outputs.size());

    if (outputs.isEmpty()) {
      // Nothing to submit
      return;
    }

    QueueEntry[] queueEntries = Iterables.toArray(Iterables.transform(outputs, queueEntryGenerator),
                                                  QueueEntry.class);
    agent.submit(new QueueEnqueue(queueProducer, queueName.toBytes(), queueEntries));
  }

  private final class DataObjectToQueueEntry implements Function<DataObject<T>, QueueEntry> {
    @Nullable
    @Override
    public QueueEntry apply(DataObject<T> input) {
      try {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(schemaHash);
        writer.encode(input.getData(), new BinaryEncoder(output));
        return new QueueEntry(Maps.transformValues(input.getPartitions(), PARTITION_MAP_TRANSFORMER),
                              output.toByteArray());
      } catch (IOException e) {
        // This should never happen.
        throw Throwables.propagate(e);
      }
    }
  }

  private static final class PartitionMapTransformer implements Function<Object, Integer> {
    @Nullable
    @Override
    public Integer apply(Object input) {
      return input == null ? 0 : input.hashCode();
    }
  }
}
