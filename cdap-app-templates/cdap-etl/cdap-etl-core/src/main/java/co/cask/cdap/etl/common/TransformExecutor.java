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

package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.Destroyable;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.Transformation;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Executes Transforms one iteration at a time, tracking how many records were input into and output from
 * each transform.
 *
 * @param <IN> the type of input object to the first transform
 *
 */
public class TransformExecutor<IN> implements Destroyable {

  private final Set<String> startingPoints;
  private final Map<String, TransformDetail> transformDetailMap;

  public TransformExecutor(Map<String, TransformDetail> transformDetailMap, Set<String> startingPoints) {
    this.transformDetailMap = transformDetailMap;
    this.startingPoints = startingPoints;
  }

  public TransformResponse runOneIteration(IN input) throws Exception {
    for (String stageName : startingPoints) {
      executeTransformation(stageName, ImmutableList.of(input));
    }

    Map<String, Collection<Object>> terminalNodeEntriesMap = new HashMap<>();
    Map<String, Collection<InvalidEntry<Object>>> errors = new HashMap<>();

    for (Map.Entry<String, TransformDetail> transformDetailEntry : transformDetailMap.entrySet()) {
      if (transformDetailEntry.getValue().getNextStages().isEmpty()) {
        // terminal node
        Collection<Object> entries = transformDetailEntry.getValue().getEntries();
        if (entries != null) {
          terminalNodeEntriesMap.put(transformDetailEntry.getKey(), entries);
        }
      }

      if (!transformDetailEntry.getValue().getErrors().isEmpty()) {
        errors.put(transformDetailEntry.getKey(), transformDetailEntry.getValue().getErrors());
      }
    }
    return new TransformResponse(terminalNodeEntriesMap, errors);
  }

  private <T> void executeTransformation(final String stageName, Collection<T> input) throws Exception {
    if (input == null) {
      return;
    }

    TransformDetail transformDetail = transformDetailMap.get(stageName);
    Transformation<T, Object> transformation = transformDetail.getTransformation();


    // clear old data for this stageName if its not a terminal node
    if (!transformDetail.getNextStages().isEmpty()) {
      transformDetail.getEntries().clear();
    }

    for (T inputEntry : input) {
      transformation.transform(inputEntry, transformDetail);
    }

    for (String nextStage : transformDetail.getNextStages()) {
      executeTransformation(nextStage, transformDetail.getEntries());
    }

  }

  public void resetEmitter() {
    for (TransformDetail transformDetailEntry : transformDetailMap.values()) {
      transformDetailEntry.resetEmitter();
    }
  }

  @Override
  public void destroy() {
    for (TransformDetail transformDetailEntry : transformDetailMap.values()) {
      transformDetailEntry.destroy();
    }
  }
}
