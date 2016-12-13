/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.batch.PipeTransformDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink emitter which writes to sink
 * @param <KEY_OUT> type of output key
 * @param <VAL_OUT> type of output value
 */
public class SinkEmitter<KEY_OUT, VAL_OUT> implements PipeEmitter<PipeTransformDetail> {
  private static final Logger LOG = LoggerFactory.getLogger(SinkEmitter.class);
  private final OutputWriter outputWriter;
  private final String stageName;

  public SinkEmitter(String stageName, OutputWriter<KEY_OUT, VAL_OUT> outputWriter) {
    this.outputWriter = outputWriter;
    this.stageName = stageName;
  }

  @Override
  public void emit(Object value) {
    try {
      outputWriter.write(stageName, (KeyValue<Object, Object>) value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void emitError(InvalidEntry invalidEntry) {
    // Not supported - This should never happen
    LOG.error("Emitting errors from sink {} is not supported", stageName);
  }

  @Override
  public void addTransformDetail(String stageName, PipeTransformDetail pipeTransformDetail) {
    throw new IllegalStateException(String.format("Sink {} should not have any output, but it has output {}",
                                                  this.stageName, stageName));
  }
}
