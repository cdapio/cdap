/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.internal.api.io.Schema;
import com.continuuity.app.queue.InputDatum;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.internal.app.runtime.OutputSubmitter;
import com.continuuity.internal.app.runtime.PostProcess;
import com.continuuity.internal.app.runtime.TransactionAgentSupplier;
import com.continuuity.internal.io.ByteBufferInputStream;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

/**
 *
 */
@NotThreadSafe
public final class ReflectionProcessMethod<T> implements ProcessMethod {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReflectionProcessMethod.class);

  private final Flowlet flowlet;
  private final BasicFlowletContext flowletContext;
  private final Method method;
  private final SchemaCache schemaCache;
  private final TransactionAgentSupplier txAgentSupplier;
  private final OutputSubmitter outputSubmitter;
  private final boolean hasParam;
  private final boolean needContext;
  private final ReflectionDatumReader<T> datumReader;
  private final ByteBufferInputStream byteBufferInput;
  private final BinaryDecoder decoder;

  public static <T> ReflectionProcessMethod<T> create(Flowlet flowlet, BasicFlowletContext flowletContext,
                                                      Method method,
                                                      TypeToken<T> dataType,
                                                      Schema schema, SchemaCache schemaCache,
                                                      TransactionAgentSupplier txAgentSupplier,
                                                      OutputSubmitter outputSubmitter) {
    return new ReflectionProcessMethod<T>(flowlet, flowletContext, method, dataType, schema,
                                          schemaCache, txAgentSupplier, outputSubmitter);
  }

  private ReflectionProcessMethod(Flowlet flowlet, BasicFlowletContext flowletContext,
                                  Method method,
                                  TypeToken<T> dataType,
                                  Schema schema, SchemaCache schemaCache,
                                  TransactionAgentSupplier txAgentSupplier,
                                  OutputSubmitter outputSubmitter) {
    this.flowlet = flowlet;
    this.flowletContext = flowletContext;
    this.method = method;
    this.schemaCache = schemaCache;
    this.txAgentSupplier = txAgentSupplier;
    this.outputSubmitter = outputSubmitter;

    this.hasParam = method.getGenericParameterTypes().length > 0;
    this.needContext = method.getGenericParameterTypes().length == 2;
    this.datumReader = new ReflectionDatumReader<T>(schema, dataType);
    this.byteBufferInput = new ByteBufferInputStream(null);
    this.decoder = new BinaryDecoder(byteBufferInput);

    if(!this.method.isAccessible()) {
      this.method.setAccessible(true);
    }
  }

  @Override
  public boolean needsInput() {
    return hasParam;
  }

  @Override
  public PostProcess invoke(InputDatum input) {
    return doInvoke(input);
  }

  @Override
  public String toString() {
    return flowlet.getClass() + "." + method.toString();
  }

  private PostProcess doInvoke(final InputDatum input) {
    final TransactionAgent txAgent = txAgentSupplier.createAndUpdateProxy();

    try {
      txAgent.start();
      Preconditions.checkState(!hasParam || input.needProcess(), "Empty input provided to method that needs input.");

      T event = null;
      if (hasParam) {
        ByteBuffer data = input.getData();
        Schema sourceSchema = schemaCache.get(data);
        Preconditions.checkNotNull(sourceSchema, "Fail to find source schema.");

        byteBufferInput.reset(data);
        event = datumReader.read(decoder, sourceSchema);

      }
      InputContext inputContext = input.getInputContext();

      try {
        if (hasParam) {
          if(needContext) {
            method.invoke(flowlet, event, inputContext);
          } else if (hasParam) {
            method.invoke(flowlet, event);
          }
        } else {
          method.invoke(flowlet);
        }
        outputSubmitter.submit(txAgent);

        return getPostProcess(txAgent, input, event, inputContext);

      } catch(Throwable t) {
        return getFailurePostProcess(t, txAgent, input, event, inputContext);
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
              LOGGER.error("Fail to commit transction: " + input, t);
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

    private final TransactionAgentSupplier txAgentSupplier;
    private final InputDatum input;

    private SimpleInputAcknowledger(TransactionAgentSupplier txAgentSupplier, InputDatum input) {
      this.txAgentSupplier = txAgentSupplier;
      this.input = input;
    }

    @Override
    public void ack() throws OperationException {
      TransactionAgent txAgent = txAgentSupplier.create();
      txAgent.start();
      input.submitAck(txAgent);
      txAgent.finish();
    }
  }
}
