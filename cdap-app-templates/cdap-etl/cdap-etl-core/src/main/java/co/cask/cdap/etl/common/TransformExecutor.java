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

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.Destroyable;
import co.cask.cdap.etl.api.Emitter;
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
  private final Map<String, Transformation> trackedTransformations;
  private final DefaultEmitter defaultEmitter;

  public TransformExecutor(Map<String, Transformation> transformationMap, Metrics metrics,
                           Map<String, List<String>> connectionsMap, String start) {
    this.start = start;
    this.connectionsMap = connectionsMap;
    this.trackedTransformations = new HashMap<>();
    for (Map.Entry<String, Transformation> transformationEntry : transformationMap.entrySet()) {
      trackedTransformations.put(transformationEntry.getKey(),
                                new TrackedTransform<>(transformationEntry.getValue(),
                                                       new DefaultStageMetrics(metrics, transformationEntry.getKey())));
    }
    defaultEmitter = new DefaultEmitter(metrics);
  }

  public TransformResponse runOneIteration(IN input) throws Exception {
    if (trackedTransformations.containsKey(start)) {
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

  private <T> void executeTransformation(final String stageName, List<T> input) throws Exception {
    Transformation<T, Object> transformation = trackedTransformations.get(stageName);

    // clear old data for this stageName if its not a terminal node
    if (input == null) {
      return;
    }

    if (connectionsMap.containsKey(stageName) && defaultEmitter.getEntriesMap().containsKey(stageName)) {
      // clear old data if this node was used in a different path earlier during execution.
      defaultEmitter.getEntries(stageName).clear();
    }

    if (trackedTransformations.containsKey(stageName)) {
      // has transformation (could be source or transform or sink)
      for (T inputEntry : input) {
        transformation.transform(inputEntry, new Emitter<Object>() {
          @Override
          public void emit(Object value) {
            defaultEmitter.emit(stageName, value);
          }

          @Override
          public void emitError(InvalidEntry<Object> invalidEntry) {
            defaultEmitter.emitError(stageName, invalidEntry);
          }
        });
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
      if (!trackedTransformations.containsKey(stageName)) {
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
    for (Transformation transformation : trackedTransformations.values()) {
      if (transformation instanceof Destroyable) {
        Destroyables.destroyQuietly((Destroyable) transformation);
      }
    }
  }
}
