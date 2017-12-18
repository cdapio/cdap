/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.batch;

import co.cask.cdap.etl.api.Alert;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.ErrorRecord;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.MultiOutputEmitter;
import co.cask.cdap.etl.common.BasicErrorRecord;
import co.cask.cdap.etl.common.RecordInfo;
import co.cask.cdap.etl.common.RecordType;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An Emitter that emits records to the next stages without buffering anything in memory. This means that within
 * the transform method of one stage, another stage's transform method can be called.
 *
 * This class always emits RecordInfo for output.
 */
public class PipeEmitter implements Emitter<Object>, MultiOutputEmitter<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(PipeEmitter.class);
  protected final String stageName;
  private final Set<PipeStage<RecordInfo>> outputConsumers;
  // port -> set of stages connected to that port
  private final Multimap<String, PipeStage<RecordInfo>> outputPortConsumers;
  private final Set<PipeStage<RecordInfo<ErrorRecord<Object>>>> errorConsumers;
  private final Set<PipeStage<RecordInfo<Alert>>> alertConsumers;
  private boolean logWarning;

  public PipeEmitter(String stageName,
                     Set<PipeStage<RecordInfo>> outputConsumers,
                     Multimap<String, PipeStage<RecordInfo>> outputPortConsumers,
                     Set<PipeStage<RecordInfo<ErrorRecord<Object>>>> errorConsumers,
                     Set<PipeStage<RecordInfo<Alert>>> alertConsumers) {
    this.stageName = stageName;
    this.outputConsumers = ImmutableSet.copyOf(outputConsumers);
    this.outputPortConsumers = ImmutableMultimap.copyOf(outputPortConsumers);
    this.errorConsumers = ImmutableSet.copyOf(errorConsumers);
    this.alertConsumers = ImmutableSet.copyOf(alertConsumers);
    this.logWarning = true;
  }

  @Override
  public void emit(String port, Object value) {
    if (port == null) {
      throw new IllegalArgumentException("Port cannot be null.");
    }
    RecordInfo record = getPipeRecord(value);
    for (PipeStage<RecordInfo> outputPortConsumer : outputPortConsumers.get(port)) {
      outputPortConsumer.consume(record);
    }
  }

  @Override
  public void emit(Object value) {
    RecordInfo record = getPipeRecord(value);
    for (PipeStage<RecordInfo> outputConsumer : outputConsumers) {
      outputConsumer.consume(record);
    }
  }

  @Override
  public void emitError(InvalidEntry<Object> invalidEntry) {
    if (logWarning && errorConsumers.isEmpty()) {
      logWarning = false;
      LOG.warn("Stage {} emits error records, but has no error consumer. Error records will be dropped.", stageName);
      return;
    }

    ErrorRecord<Object> errorRecord = new BasicErrorRecord<>(invalidEntry.getInvalidRecord(), stageName,
                                                             invalidEntry.getErrorCode(), invalidEntry.getErrorMsg());
    RecordInfo<ErrorRecord<Object>> errorRecordInfo =
      RecordInfo.builder(errorRecord, stageName, RecordType.ERROR).build();
    for (PipeStage<RecordInfo<ErrorRecord<Object>>> pipeTransform : errorConsumers) {
      pipeTransform.consume(errorRecordInfo);
    }
  }

  @Override
  public void emitAlert(Map<String, String> payload) {
    Alert alert = new Alert(stageName, ImmutableMap.copyOf(payload));
    RecordInfo<Alert> alertRecord = RecordInfo.builder(alert, stageName, RecordType.ALERT).build();
    for (PipeStage<RecordInfo<Alert>> alertConsumer : alertConsumers) {
      alertConsumer.consume(alertRecord);
    }
  }

  protected RecordInfo getPipeRecord(Object value) {
    return RecordInfo.builder(value, stageName, RecordType.OUTPUT).build();
  }

  /**
   * Get a builder to create a PipeEmitter for the specified stage
   *
   * @param stageName the stage name
   * @return a builder to create a PipeEmitter for the specified stage
   */
  public static Builder builder(String stageName) {
    return new Builder(stageName);
  }

  /**
   * Base implementation of a Builder for a PipeEmitter.
   */
  public static class Builder {
    protected final String stageName;
    protected final Multimap<String, PipeStage<RecordInfo>> outputPortConsumers;
    protected final Set<PipeStage<RecordInfo<ErrorRecord<Object>>>> errorConsumers;
    protected final Set<PipeStage<RecordInfo>> outputConsumers;
    protected final Set<PipeStage<RecordInfo<Alert>>> alertConsumers;

    protected Builder(String stageName) {
      this.stageName = stageName;
      this.outputPortConsumers = HashMultimap.create();
      this.outputConsumers = new HashSet<>();
      this.errorConsumers = new HashSet<>();
      this.alertConsumers = new HashSet<>();
    }

    public Builder addOutputConsumer(PipeStage<RecordInfo> outputConsumer) {
      outputConsumers.add(outputConsumer);
      return this;
    }

    public Builder addOutputConsumer(PipeStage<RecordInfo> outputConsumer, String port) {
      outputPortConsumers.put(port, outputConsumer);
      return this;
    }

    public Builder addErrorConsumer(PipeStage<RecordInfo<ErrorRecord<Object>>> errorConsumer) {
      errorConsumers.add(errorConsumer);
      return this;
    }

    public Builder addAlertConsumer(PipeStage<RecordInfo<Alert>> alertConsumer) {
      alertConsumers.add(alertConsumer);
      return this;
    }

    public PipeEmitter build() {
      return new PipeEmitter(stageName, outputConsumers, outputPortConsumers, errorConsumers, alertConsumers);
    }
  }
}
