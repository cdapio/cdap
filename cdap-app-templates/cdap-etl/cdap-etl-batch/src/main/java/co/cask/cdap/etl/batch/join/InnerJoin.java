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
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.JoinResult;
import co.cask.cdap.etl.api.Joiner;
import co.cask.cdap.etl.batch.mapreduce.MapTaggedOutputWritable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Performs Inner join
 * @param <JOIN_KEY> type of join key
 * @param <JOIN_VALUE> type of join value
 * @param <OUT> type of output of mapreduce
 */
public class InnerJoin<JOIN_KEY, JOIN_VALUE, OUT> implements Join {
  private Joiner<JOIN_KEY, JOIN_VALUE, OUT> joiner;
  private JOIN_KEY joinKey;
  private Iterator<MapTaggedOutputWritable<JOIN_VALUE>> iterator;
  private Emitter<OUT> emitter;

  public InnerJoin(Joiner<JOIN_KEY, JOIN_VALUE, OUT> joiner, JOIN_KEY joinKey,
                   Iterator<MapTaggedOutputWritable<JOIN_VALUE>> iterator, Emitter<OUT> emitter) throws Exception {
    this.joiner = joiner;
    this.joinKey = joinKey;
    this.iterator = iterator;
    this.emitter = emitter;
  }

  @Override
  public void join() throws Exception {
    Map<String, List<JoinElement>> perStageJoinElements = getPerStageJoinElements();
    innerJoin(perStageJoinElements, joiner.getJoinConfig().getNumOfInputs());
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

  private void innerJoin(Map<String, List<JoinElement>> perStageJoinElements, int noOfStages) throws Exception {
    // As we get intersection of records from n stages in inner join, if the number of stages we got after reduce
    // are not same as number of stages we are joining, then there is nothing to emit.
    if (perStageJoinElements.size() != noOfStages) {
      return;
    }

    List<List<JoinElement>> list = new ArrayList<>(perStageJoinElements.values());
    // cross product of each stage record
    ArrayList<JoinElement> joinElements = new ArrayList<>();
    ArrayList<JoinResult> joinResults = new ArrayList<>();
    getCartesianProduct(list, 0, noOfStages, joinElements, joinResults);
    joiner.merge(joinKey, joinResults, emitter);
  }

  // TODO use iterative algorithm instead of recursion
  void getCartesianProduct(List<List<JoinElement>> list, int index, int size, List<JoinElement> joinElements,
                           List<JoinResult> joinResults) {
    if (joinElements.size() == size) {
      JoinResult joinResult = new JoinResult(joinElements);
      joinResults.add(joinResult);
      return;
    }

    List<JoinElement> joinElementList = list.get(index);
    for (int i = 0; i < joinElementList.size(); i++) {
      joinElements.add(joinElementList.get(i));
      getCartesianProduct(list, index + 1, size, joinElements, joinResults);
      joinElements.remove(joinElements.size() - 1);
    }
  }
}
