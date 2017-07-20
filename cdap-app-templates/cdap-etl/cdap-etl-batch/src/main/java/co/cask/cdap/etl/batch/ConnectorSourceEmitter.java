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
import co.cask.cdap.etl.api.ErrorRecord;
import co.cask.cdap.etl.batch.mapreduce.ErrorOutputWriter;
import co.cask.cdap.etl.common.RecordInfo;
import com.google.common.collect.Multimap;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * Just like a PipeEmitter, except it doesn't need to add stage name itself, since the ConnectorSource does it.
 */
public class ConnectorSourceEmitter extends PipeEmitter {

  // stageName passed into the constructor will be the name of the connector, something like 'myagg.connector'.
  // however, it is not used anywhere by this emitter, and is only here because the superclass requires it.
  private ConnectorSourceEmitter(String stageName,
                                 Set<PipeStage<RecordInfo>> outputConsumers,
                                 Multimap<String, PipeStage<RecordInfo>> outputPortConsumers,
                                 Set<PipeStage<RecordInfo<ErrorRecord<Object>>>> errorConsumers,
                                 Set<PipeStage<RecordInfo<Alert>>> alertConsumers,
                                 @Nullable ErrorOutputWriter<Object, Object> errorOutputWriter) {
    super(stageName, outputConsumers, outputPortConsumers, errorConsumers, alertConsumers, errorOutputWriter);
  }

  // we expect the value to already be a RecordInfo. This is because ConnectorSource emits RecordInfo,
  // which it has to be responsible for because the stage name associated with the record is stored in the connector
  // local dataset.
  protected RecordInfo getPipeRecord(Object value) {
    return (RecordInfo) value;
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
  public static class Builder extends PipeEmitter.Builder {

    private Builder(String stageName) {
      super(stageName);
    }

    @Override
    public PipeEmitter build() {
      return new ConnectorSourceEmitter(stageName, outputConsumers, outputPortConsumers,
                                        errorConsumers, alertConsumers, errorOutputWriter);
    }
  }
}
