package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.io.Schema;
import com.continuuity.app.queue.QueueName;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.internal.app.runtime.EmittedDatum;
import com.continuuity.internal.app.runtime.OutputSubmitter;
import com.continuuity.internal.io.ReflectionDatumWriter;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 */
public final class ReflectionOutputEmitter implements OutputEmitter<Object>, OutputSubmitter {

  private static final Function<EmittedDatum, WriteOperation> DATUM_TO_WRITE_OP = EmittedDatum.datumToWriteOp();

  private final BasicFlowletContext flowletContext;
  private final QueueProducer queueProducer;
  private final QueueName queueName;
  private final byte[] schemaHash;
  private final ReflectionDatumWriter writer;
  private final BlockingQueue<EmittedDatum> dataQueue;

  public ReflectionOutputEmitter(QueueProducer queueProducer, QueueName queueName,
                                 Schema schema, BasicFlowletContext flowletContext) {
    this.queueProducer = queueProducer;
    this.queueName = queueName;
    this.schemaHash = schema.getSchemaHash().toByteArray();
    this.writer = new ReflectionDatumWriter(schema);
    this.dataQueue = new LinkedBlockingQueue<EmittedDatum>();
    this.flowletContext = flowletContext;
  }

  @Override
  public void emit(Object data) {
    emit(data, ImmutableMap.<String, Object>of());
  }

  @Override
  public void emit(Object data, Map<String, Object> partitions) {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      output.write(schemaHash);
      writer.write(data, new BinaryEncoder(output));
      dataQueue.add(new EmittedDatum(queueProducer, queueName, output.toByteArray(), partitions));
    } catch(IOException e) {
      // This should never happens.
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void submit(TransactionAgent agent) throws OperationException {
    List<EmittedDatum> outputs = Lists.newArrayListWithExpectedSize(dataQueue.size());
    dataQueue.drainTo(outputs);

    flowletContext.getSystemMetrics().counter(queueName.getSimpleName() + ".stream.out", outputs.size());

    agent.submit(ImmutableList.copyOf(Iterables.transform(outputs, DATUM_TO_WRITE_OP)));
  }
}
