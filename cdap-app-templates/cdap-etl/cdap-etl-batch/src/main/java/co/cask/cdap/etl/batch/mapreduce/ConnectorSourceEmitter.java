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
import co.cask.cdap.etl.api.batch.BatchEmitter;
import co.cask.cdap.etl.batch.BatchTransformDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Connector source emitter which converts emitted value into {@link KeyValue} because connect sink stores record
 * along with stageName
 */
public class ConnectorSourceEmitter extends BatchEmitter<BatchTransformDetail> {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectorSourceEmitter.class);

  private final String stageName;
  private final Map<String, BatchTransformDetail> nextStages;
  @Nullable
  private final ErrorOutputWriter<Object, Object> errorOutputWriter;

  public ConnectorSourceEmitter(String stageName, Map<String, BatchTransformDetail> nextStages) {
    this(stageName, nextStages, null);
  }

  public ConnectorSourceEmitter(String stageName, Map<String, BatchTransformDetail> nextStages,
                                @Nullable ErrorOutputWriter<Object, Object> errorOutputWriter) {
    this.stageName = stageName;
    this.nextStages = nextStages;
    this.errorOutputWriter = errorOutputWriter;
  }

  @Override
  public void addTransformDetail(String stageName, BatchTransformDetail transformDetail) {
    nextStages.put(stageName, transformDetail);
  }

  @Nullable
  @Override
  public Map<String, BatchTransformDetail> getNextStages() {
    return nextStages;
  }

  @Override
  public void emit(Object value) {
    for (BatchTransformDetail batchTransformDetail : nextStages.values()) {
      batchTransformDetail.process((KeyValue<String, Object>) value);
    }
  }

  @Override
  public void emitError(InvalidEntry<Object> invalidEntry) {
    // Not supported - This should never happen
    LOG.error("Emitting errors from stage {} is not supported", stageName);
  }
}
