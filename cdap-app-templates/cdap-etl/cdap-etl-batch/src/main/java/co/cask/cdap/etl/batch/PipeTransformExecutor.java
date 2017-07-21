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

import co.cask.cdap.etl.api.Destroyable;
import co.cask.cdap.etl.common.RecordInfo;

import java.util.Map;
import java.util.Set;

/**
 * Executes chain of transforms
 *
 * @param <IN> Type of input
 */
public class PipeTransformExecutor<IN> implements Destroyable {
  private final Set<String> startingPoints;
  private final Map<String, PipeStage> pipeStages;

  public PipeTransformExecutor(Map<String, PipeStage> pipeStages, Set<String> startingPoints) {
    this.pipeStages = pipeStages;
    this.startingPoints = startingPoints;
  }

  public void runOneIteration(IN input) {
    for (String stageName : startingPoints) {
      PipeStage<RecordInfo> pipeStage = pipeStages.get(stageName);
      pipeStage.consume(RecordInfo.builder(input, stageName).build());
    }
  }

  @Override
  public void destroy() {
    for (PipeStage stage : pipeStages.values()) {
      stage.destroy();
    }
  }
}
