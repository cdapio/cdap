/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.app.queue.InputDatum;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.internal.app.runtime.DataFabricFacade;
import com.continuuity.internal.app.runtime.OutputSubmitter;
import com.continuuity.internal.app.runtime.PostProcess;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.Executor;

/**
 *
 */
@NotThreadSafe
public final class ReflectionProcessMethod<T> implements ProcessMethod<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReflectionProcessMethod.class);

  private final Flowlet flowlet;
  private final BasicFlowletContext flowletContext;
  private final Method method;
  private final DataFabricFacade txAgentSupplier;
  private final OutputSubmitter outputSubmitter;
  private final boolean hasParam;
  private final boolean needsBatch;
  private final boolean needContext;

  public static ReflectionProcessMethod create(Flowlet flowlet, BasicFlowletContext flowletContext,
                                                      Method method,
                                                      DataFabricFacade txAgentSupplier,
                                                      OutputSubmitter outputSubmitter) {
    return new ReflectionProcessMethod(flowlet, flowletContext, method, txAgentSupplier, outputSubmitter);
  }

  private ReflectionProcessMethod(Flowlet flowlet, BasicFlowletContext flowletContext,
                                  Method method,
                                  DataFabricFacade txAgentSupplier,
                                  OutputSubmitter outputSubmitter) {
    this.flowlet = flowlet;
    this.flowletContext = flowletContext;
    this.method = method;
    this.txAgentSupplier = txAgentSupplier;
    this.outputSubmitter = outputSubmitter;

    this.hasParam = method.getGenericParameterTypes().length > 0;
    this.needsBatch = hasParam &&
      TypeToken.of(method.getGenericParameterTypes()[0]).getRawType().equals(Iterator.class);
    this.needContext = method.getGenericParameterTypes().length == 2;

    if(!this.method.isAccessible()) {
      this.method.setAccessible(true);
    }
  }

  @Override
  public boolean needsInput() {
    return hasParam;
  }

  @Override
  public PostProcess invoke(InputDatum input, Function<ByteBuffer, T> inputDatumTransformer) {
    return doInvoke(input, inputDatumTransformer);
  }

  @Override
  public String toString() {
    return flowlet.getClass() + "." + method.toString();
  }

  private PostProcess doInvoke(final InputDatum input, final Function<ByteBuffer, T> inputDatumTransformer) {
    final TransactionAgent txAgent = txAgentSupplier.createAndUpdateTransactionAgentProxy();

    try {
      txAgent.start();
      Preconditions.checkState(!hasParam || input.needProcess(), "Empty input provided to method that needs input.");

      T event = null;
      if (hasParam) {
        Iterator<ByteBuffer> inputIterator = input.getData();
        Iterator<T> dataIterator = Iterators.transform(inputIterator, inputDatumTransformer);

        if(needsBatch) {
          //noinspection unchecked
          event = (T) dataIterator;
        } else {
          event = dataIterator.next();
        }
      }
      InputContext inputContext = input.getInputContext();

      try {
        if (hasParam) {
          if(needContext) {
            method.invoke(flowlet, event, inputContext);
          } else {
            method.invoke(flowlet, event);
          }
        } else {
          method.invoke(flowlet);
        }
        return getPostProcess(txAgent, input, event, inputContext);
      } catch(Throwable t) {
        return getFailurePostProcess(t, txAgent, input, event, inputContext);
      } finally {
        outputSubmitter.submit(txAgent);
      }
    } catch (Exception e) {
      // If it reaches here, something very wrong.
      LOGGER.error("Fail to process input: " + method, e);
      throw Throwables.propagate(e);
    }
  }

  private PostProcess getPostProcess(final TransactionAgent txAgent,
                                     final InputDatum input,
                                     final T event,
                                     final InputContext inputContext) {
    return new PostProcess() {
      @Override
      public void commit(Executor executor, final Callback callback) {
        executor.execute(new Runnable() {

          @Override
          public void run() {
            try {
              input.submitAck(txAgent);
              txAgent.finish();
              callback.onSuccess(event, inputContext);
            } catch (Throwable t) {
              LOGGER.error("Fail to commit transaction: " + input, t);
              callback.onFailure(event, inputContext,
                                 new FailureReason(FailureReason.Type.IO_ERROR, t.getMessage(), t),
                                 new SimpleInputAcknowledger(txAgentSupplier, input));
            } finally {
              // we want to emit metrics after every retry after finish() so that deferred operations are also logged
              flowletContext.getMetrics().count("dataops", txAgent.getSucceededCount());
            }
          }
        });
      }
    };
  }

  private PostProcess getFailurePostProcess(final Throwable t,
                                            final TransactionAgent txAgent,
                                            final InputDatum input,
                                            final T event,
                                            final InputContext inputContext) {
    return new PostProcess() {
      @Override
      public void commit(Executor executor, final Callback callback) {
        executor.execute(new Runnable() {

          @Override
          public void run() {
            try {
              // emitting metrics before abort is fine: we don't perform any operataions during abort();
              flowletContext.getMetrics().count("dataops", txAgent.getSucceededCount());
              txAgent.abort();
            } catch (Throwable t) {
              LOGGER.error("OperationException when aborting transaction.", t);
            } finally {
              callback.onFailure(event, inputContext,
                                 new FailureReason(FailureReason.Type.USER, t.getMessage(), t),
                                 new SimpleInputAcknowledger(txAgentSupplier, input));
            }
          }
        });
      }
    };

  }

  private static final class SimpleInputAcknowledger implements PostProcess.InputAcknowledger {

    private final DataFabricFacade txAgentSupplier;
    private final InputDatum input;

    private SimpleInputAcknowledger(DataFabricFacade txAgentSupplier, InputDatum input) {
      this.txAgentSupplier = txAgentSupplier;
      this.input = input;
    }

    @Override
    public void ack() throws OperationException {
      TransactionAgent txAgent = txAgentSupplier.createTransactionAgent();
      txAgent.start();
      input.submitAck(txAgent);
      txAgent.finish();
    }
  }
}
