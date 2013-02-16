package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
class PostProcessMethod {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessMethod.class);

  private final OperationContext operationCtx;
  private final OperationExecutor opex;

  PostProcessMethod(OperationContext operationCtx, OperationExecutor opex) {
    this.opex = opex;
    this.operationCtx = operationCtx;
  }

  public void postProcess(Collection<ManagedOutputEmitter<?>> outputEmitters, ManagedBatchCollector batchCollector) {
    postProcess(null, outputEmitters, batchCollector);
  }

  public void postProcess(InputDatum input, Collection<ManagedOutputEmitter<?>> outputEmitters,
                          ManagedBatchCollector batchCollector) {

    EmittedDatumIterable emittedDatums = new EmittedDatumIterable(outputEmitters);
    List<WriteOperation> writeOperations = Lists.newLinkedList(batchCollector.capture());

    // Creates write operations for each datum emitted
    for (EmittedDatum datum : emittedDatums) {
      // TODO: Metrics
      writeOperations.add(datum.asEnqueue());
    }

    // TODO: Metrics

    if (input != null) {
      writeOperations.add(input.asAck());
    }

    // Check if there is nothing to do
    if (writeOperations.isEmpty()) {
      // TODO, return something
    }

    try {
      opex.execute(operationCtx, writeOperations);
    } catch (OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  private static final class EmittedDatumIterable implements Iterable<EmittedDatum> {

    private final Iterable<EmittedDatum> iterable;

    EmittedDatumIterable(Collection<ManagedOutputEmitter<?>> emitters) {
      List<Iterable<EmittedDatum>> captured = Lists.newArrayListWithCapacity(emitters.size());
      for (ManagedOutputEmitter<?> emitter : emitters) {
        captured.add(emitter.capture());
      }
      iterable = Iterables.concat(captured);
    }

    @Override
    public Iterator<EmittedDatum> iterator() {
      return iterable.iterator();
    }
  }
}
