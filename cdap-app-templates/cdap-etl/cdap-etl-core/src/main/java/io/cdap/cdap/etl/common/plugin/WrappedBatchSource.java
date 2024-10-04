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
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Wrapper around {@link BatchSource} that makes sure logging, classloading, and other pipeline
 * capabilities are setup correctly.
 *
 * @param <KEY_IN> the input key type
 * @param <VAL_IN> the input value type
 * @param <OUT> the output type
 */
public class WrappedBatchSource<KEY_IN, VAL_IN, OUT>
    extends BatchSource<KEY_IN, VAL_IN, OUT>
    implements PluginWrapper<BatchSource<KEY_IN, VAL_IN, OUT>> {

  private final BatchSource<KEY_IN, VAL_IN, OUT> batchSource;
  private final Caller caller;
  private final OperationTimer operationTimer;
  private static final Logger LOG = LoggerFactory.getLogger(WrappedBatchSource.class);

  public WrappedBatchSource(BatchSource<KEY_IN, VAL_IN, OUT> batchSource,
      Caller caller, OperationTimer operationTimer) {
    this.batchSource = batchSource;
    this.caller = caller;
    this.operationTimer = operationTimer;
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    caller.call((Callable<Void>) () -> {
      batchSource.prepareRun(context);
      return null;
    });
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    caller.call((Callable<Void>) () -> {
      batchSource.initialize(context);
      return null;
    });
  }

  @Override
  public void transform(KeyValue<KEY_IN, VAL_IN> input, Emitter<OUT> emitter) throws Exception {
    operationTimer.start();
    try {
      caller.call((Callable<Void>) () -> {
        batchSource.transform(input, new UntimedEmitter<>(emitter, operationTimer));
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
      batchSource.destroy();
      return null;
    });
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    caller.callUnchecked((Callable<Void>) () -> {
      batchSource.onRunFinish(succeeded, context);
      return null;
    });
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    caller.callUnchecked((Callable<Void>) () -> {
      batchSource.configurePipeline(pipelineConfigurer);
      return null;
    });
  }

  @Override
  public BatchSource<KEY_IN, VAL_IN, OUT> getWrapped() {
    return batchSource;
  }
}
