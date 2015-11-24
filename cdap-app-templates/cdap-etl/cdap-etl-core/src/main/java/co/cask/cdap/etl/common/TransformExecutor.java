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
    executeTransfomation(start, ImmutableList.of(input));

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

  // this will be called with starting stage name
  // we will get the transformation of it and also check if this is a terminal node
  // if its not a terminal node
  //    1) execute the transformation
  //    2) using the connections map, recursively call the executeTransformation on the next links
  // else if its a terminal node
  //    1) if transformation exists, execute the transformation, else go to step-2
  //    2) return
  private <T> void executeTransfomation(String stageName, List<T> input) throws Exception {
    Transformation<T, Object> transformation = trackedTransformDetail.getTransformation(stageName);
    if (transformation == null) {
      // could be either source or sink
      // get the next connections, if they are not empty, its a source
      List<String> nextStages = connectionsMap.get(stageName);
      if (nextStages != null) {
        // source
        for (String nextStageName : nextStages) {
          executeTransfomation(nextStageName, input);
        }
      } else {
        // its a sink, add the input to the emitter entries list
        for (T inputEntry : input) {
          defaultEmitter.emit(stageName, inputEntry);
        }
      }
    } else {
      // has transformation
      if (input != null) {
        // has input
        for (T inputEntry : input) {
          transformation.transform(inputEntry, defaultEmitter);
        }
      }
      List<String> nextStages = connectionsMap.get(stageName);
      if (nextStages != null) {
        // transform
        for (String nextStage : nextStages) {
          executeTransfomation(nextStage, defaultEmitter.getEntries(stageName));
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
