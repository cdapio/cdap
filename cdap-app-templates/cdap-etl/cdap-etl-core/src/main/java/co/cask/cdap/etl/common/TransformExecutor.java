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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Executes Transforms one iteration at a time, tracking how many records were input into and output from
 * each transform.
 *
 * @param <IN> the type of input object to the first transform
 *
 */
public class TransformExecutor<IN> implements Destroyable {

  private final String start;
  private final Map<String, List<String>> connectionsMap;
  private final TransformDetail trackedTransformDetail;
  private final DefaultEmitter<Object> defaultEmitter;

  public TransformExecutor(TransformDetail transformDetail, Map<String, List<String>> connectionsMap, String start) {
    this.start = start;
    this.connectionsMap = connectionsMap;
    Map<String, Transformation> trackedTransformation = new HashMap<>();
    for (Map.Entry<String, Transformation> transformationEntry : transformDetail.getTransformationMap().entrySet()) {
      trackedTransformation.put(transformationEntry.getKey(),
                                new TrackedTransform<>(transformationEntry.getValue(),
                                                     new DefaultStageMetrics(transformDetail.getMetrics(),
                                                                             transformationEntry.getKey())));
    }
    trackedTransformDetail = new TransformDetail(transformDetail, trackedTransformation);
    defaultEmitter = new DefaultEmitter<>(transformDetail.getMetrics());
  }

  public TransformResponse runOneIteration(IN input) throws Exception {
    if (trackedTransformDetail.getTransformationMap().containsKey(start)) {
      executeTransformation(start, ImmutableList.of(input));
    } else {
      List<String> nextToStart = connectionsMap.get(start);
      Preconditions.checkNotNull(nextToStart);

      for (String stage : nextToStart) {
        executeTransformation(stage, ImmutableList.of(input));
      }
    }

    Map<String, List<Object>> terminalNodeEntriesMap = new HashMap<>();
    Map<String, List<Object>> emitterEntries = defaultEmitter.getEntriesMap();
    for (String key : emitterEntries.keySet()) {
      if (!connectionsMap.containsKey(key)) {
        // terminal node
        terminalNodeEntriesMap.put(key, emitterEntries.get(key));
      }
    }

    Map<String, List<InvalidEntry<Object>>> errors = defaultEmitter.getErrors();
    return new TransformResponse(terminalNodeEntriesMap, errors);
  }

  private <T> void executeTransformation(String stageName, List<T> input) throws Exception {
    Transformation<T, Object> transformation = trackedTransformDetail.getTransformation(stageName);

    // clear old data for this stageName if its not a terminal node
    if (input == null) {
      return;
    }

    if (connectionsMap.containsKey(stageName) && defaultEmitter.getEntriesMap().containsKey(stageName)) {
      // clear old data if this node was used in a different path earlier during execution.
      defaultEmitter.getEntries(stageName).clear();
    }

    if (trackedTransformDetail.getTransformationMap().containsKey(stageName)) {
      // has transformation (could be source or transform or sink)
      for (T inputEntry : input) {
        transformation.transform(inputEntry, defaultEmitter);
      }
    }

    List<String> nextStages = connectionsMap.get(stageName);
    if (nextStages != null) {
      // transform or source
      for (String nextStage : nextStages) {
        executeTransformation(nextStage, defaultEmitter.getEntries(stageName));
      }
    } else {
      // terminal node, pass on the input if transformation for this terminal node is not already executed.
      if (!trackedTransformDetail.getTransformationMap().containsKey(stageName)) {
        for (T inputEntry : input) {
          defaultEmitter.emit(stageName, inputEntry);
        }
      }
    }
  }


  public void resetEmitter() {
    defaultEmitter.reset();
  }

  @Override
  public void destroy() {
    for (Transformation transformation : trackedTransformDetail.getTransformationMap().values()) {
      if (transformation instanceof Destroyable) {
        Destroyables.destroyQuietly((Destroyable) transformation);
      }
    }
  }
}
