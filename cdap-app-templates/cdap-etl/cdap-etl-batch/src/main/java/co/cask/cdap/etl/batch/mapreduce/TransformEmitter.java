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
import co.cask.cdap.etl.api.ErrorRecord;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.batch.PipeTransformDetail;
import co.cask.cdap.etl.common.BasicErrorRecord;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Emitter which emits {@link KeyValue} where key is stageName emitting the transformed record and value is
 * transformed record
 */
public class TransformEmitter implements PipeEmitter<PipeTransformDetail> {
  private static final Logger LOG = LoggerFactory.getLogger(TransformEmitter.class);
  private final String stageName;
  private final ErrorOutputWriter<Object, Object> errorOutputWriter;
  private final Map<String, PipeTransformDetail> outputConsumers;
  private final Map<String, PipeTransformDetail> errorConsumers;
  private boolean shouldLogErrorWarning;

  public TransformEmitter(String stageName, @Nullable ErrorOutputWriter<Object, Object> errorOutputWriter) {
    this.stageName = stageName;
    this.errorOutputWriter = errorOutputWriter;
    this.outputConsumers = new HashMap<>();
    this.errorConsumers = new HashMap<>();
    this.shouldLogErrorWarning = true;
  }

  @Override
  public void emit(Object value) {
    for (PipeTransformDetail pipeTransformDetail : outputConsumers.values()) {
      pipeTransformDetail.process(new KeyValue<>(stageName, value));
    }
  }

  @Override
  public void emitError(InvalidEntry<Object> invalidEntry) {
    if (shouldLogErrorWarning && errorConsumers.isEmpty() && errorOutputWriter == null) {
      shouldLogErrorWarning = false;
      LOG.warn("Stage {} emits error records, but has no error consumer. Error records will be dropped.", stageName);
      return;
    }
    for (PipeTransformDetail pipeTransformDetail : errorConsumers.values()) {
      ErrorRecord errorRecord = new BasicErrorRecord<>(invalidEntry.getInvalidRecord(), stageName,
                                                       invalidEntry.getErrorCode(), invalidEntry.getErrorMsg());
      pipeTransformDetail.process(new KeyValue<>(stageName, (Object) errorRecord));
    }
    try {
      if (errorOutputWriter != null) {
        errorOutputWriter.write(invalidEntry);
      }
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addTransformDetail(String stageName, PipeTransformDetail pipeTransformDetail) {
    if (pipeTransformDetail.isErrorConsumer()) {
      errorConsumers.put(stageName, pipeTransformDetail);
    } else {
      outputConsumers.put(stageName, pipeTransformDetail);
    }
  }
}
