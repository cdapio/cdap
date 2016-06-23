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

package co.cask.cdap.etl.batch.join;

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.JoinConfig;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.JoinResult;
import co.cask.cdap.etl.api.Joiner;
import co.cask.cdap.etl.batch.mapreduce.MapTaggedOutputWritable;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Performs outer join
 *
 * @param <JOIN_KEY> type of join key
 * @param <JOIN_VALUE> type of join value
 * @param <OUT> type of output to be emitted
 */
public class OuterJoin<JOIN_KEY, JOIN_VALUE, OUT> implements Join {
  private Joiner<JOIN_KEY, JOIN_VALUE, OUT> joiner;
  private JOIN_KEY joinKey;
  private Iterator<MapTaggedOutputWritable<JOIN_VALUE>> iterator;
  private Emitter<OUT> emitter;

  public OuterJoin(Joiner<JOIN_KEY, JOIN_VALUE, OUT> joiner, JOIN_KEY joinKey, 
                   Iterator<MapTaggedOutputWritable<JOIN_VALUE>> iterator, Emitter<OUT> emitter) {
    this.joiner = joiner;
    this.joinKey = joinKey;
    this.iterator = iterator;
    this.emitter = emitter;
  }

  @Override
  public void join() throws Exception {
    Map<String, List<JoinElement>> perStageJoinElements = getPerStageJoinElements();
    JoinConfig joinConfig = joiner.getJoinConfig();
    Iterable<String> requiredInputs = joinConfig.getRequiredInputs();

    List<List<JoinElement>> list = new ArrayList<>(perStageJoinElements.values());
    ArrayList<JoinElement> joinElements = new ArrayList<>();
    ArrayList<JoinResult> cartesianProduct = new ArrayList<>();
    ArrayList<JoinResult> joinResults = new ArrayList<>();
    getCartesianProduct(list, 0, joinElements, cartesianProduct);

    for (JoinResult joinResult : cartesianProduct) {
      List<JoinElement> joinRow = joinResult.getJoinResult();
      Iterator<String> iterator = requiredInputs.iterator();
      int count = 0;
      while (iterator.hasNext()) {
        String requiredInput = iterator.next();
        // Count number of joinElements from required inputs
        for (JoinElement joinElement : joinRow) {
          if (joinElement.getStageName().equalsIgnoreCase(requiredInput)) {
            count++;
          }
        }
      }

      // Add only those join results which has joinElement from all the requiredInputs
      if (count == Iterables.size(requiredInputs)) {
        joinResults.add(joinResult);
      }
    }

    // Merge only if join results is non empty
    if (!joinResults.isEmpty()) {
      joiner.merge(joinKey, joinResults, emitter);
    }
  }

  private Map<String, List<JoinElement>> getPerStageJoinElements() {
    Map<String, List<JoinElement>> perStageJoinElements = new HashMap<>();
    while (iterator.hasNext()) {
      MapTaggedOutputWritable<JOIN_VALUE> record = iterator.next();
      JoinElement joinElement = new JoinElement(record.getStageName(), record.getRecord());
      String stageName = record.getStageName();
      if (perStageJoinElements.get(stageName) == null) {
        perStageJoinElements.put(stageName, new ArrayList<JoinElement>());
      }
      perStageJoinElements.get(stageName).add(joinElement);
    }
    return perStageJoinElements;
  }

  // TODO use iterative algorithm instead of recursion
  void getCartesianProduct(List<List<JoinElement>> list, int index, List<JoinElement> joinElements,
                           List<JoinResult> joinResults) {
    if (index == list.size()) {
      JoinResult joinResult = new JoinResult(joinElements);
      joinResults.add(joinResult);
      return;
    }

    List<JoinElement> joinElementList = list.get(index);
    for (int i = 0; i < joinElementList.size(); i++) {
      joinElements.add(joinElementList.get(i));
      getCartesianProduct(list, index + 1, joinElements, joinResults);
      joinElements.remove(joinElements.size() - 1);
    }
  }
}
