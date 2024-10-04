/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.etl.common.plugin;

import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.exception.WrappedException;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Wrapper around {@link BatchSource} that makes sure logging, classloading, and other pipeline
 * capabilities are setup correctly.
 *
 * @param <IN> type of input
 * @param <KEY_OUT> type of output key
 * @param <VAL_OUT> type of output value
 */
public class WrappedBatchSink<IN, KEY_OUT, VAL_OUT>
    extends BatchSink<IN, KEY_OUT, VAL_OUT>
    implements PluginWrapper<BatchSink<IN, KEY_OUT, VAL_OUT>> {

  private final BatchSink<IN, KEY_OUT, VAL_OUT> batchSink;
  private final Caller caller;
  private final OperationTimer operationTimer;
  private static final Logger LOG = LoggerFactory.getLogger(WrappedBatchSink.class);

  public WrappedBatchSink(BatchSink<IN, KEY_OUT, VAL_OUT> batchSink, Caller caller,
      OperationTimer operationTimer) {
    this.batchSink = batchSink;
    this.caller = caller;
    this.operationTimer = operationTimer;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    caller.call((Callable<Void>) () -> {
      batchSink.prepareRun(context);
      return null;
    });
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    caller.call((Callable<Void>) () -> {
      batchSink.initialize(context);
      return null;
    });
  }

  @Override
  public void transform(IN input,
      Emitter<KeyValue<KEY_OUT, VAL_OUT>> emitter) throws Exception {
    operationTimer.start();
    try {
      caller.call((Callable<Void>) () -> {
        batchSink.transform(input, new UntimedEmitter<>(emitter, operationTimer));
        return null;
      });
    } catch(Exception e) {
      if (caller instanceof StageLoggingCaller) {
        String stageName = ((StageLoggingCaller) caller).getStageName();
        MDC.put("Failed_Stage", stageName);
        LOG.error("Stage: {}", stageName);
        throw new WrappedException(e, stageName);
      }
      throw e;
    } finally {
      operationTimer.reset();
    }
  }

  @Override
  public void destroy() {
    caller.callUnchecked((Callable<Void>) () -> {
      batchSink.destroy();
      return null;
    });
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    caller.callUnchecked((Callable<Void>) () -> {
      batchSink.configurePipeline(pipelineConfigurer);
      return null;
    });
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    caller.callUnchecked((Callable<Void>) () -> {
      batchSink.onRunFinish(succeeded, context);
      return null;
    });
  }

  @Override
  public BatchSink<IN, KEY_OUT, VAL_OUT> getWrapped() {
    return batchSink;
  }
}
