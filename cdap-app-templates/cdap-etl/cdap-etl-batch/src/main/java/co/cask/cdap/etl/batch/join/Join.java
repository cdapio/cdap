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
import co.cask.cdap.etl.api.Joiner;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Performs join operation
 * @param <JOIN_KEY> type of join key
 * @param <INPUT_RECORD> type of input record
 * @param <OUT> type of output of mapreduce
 */

public class Join<JOIN_KEY, INPUT_RECORD, OUT> {
  private Joiner<JOIN_KEY, INPUT_RECORD, OUT> joiner;
  private JOIN_KEY joinKey;
  private Iterator<JoinElement<INPUT_RECORD>> iterator;
  private Emitter<OUT> emitter;
  private final int numOfInputs;

  public Join(Joiner<JOIN_KEY, INPUT_RECORD, OUT> joiner, JOIN_KEY joinKey,
              Iterator<JoinElement<INPUT_RECORD>> iterator, int numOfInputs, Emitter<OUT> emitter) throws Exception {
    this.joiner = joiner;
    this.joinKey = joinKey;
    this.iterator = iterator;
    this.numOfInputs = numOfInputs;
    this.emitter = emitter;
  }

  public void joinRecords() throws Exception {
    Map<String, List<JoinElement<INPUT_RECORD>>> perStageJoinElements = getPerStageJoinElements();
    JoinConfig joinConfig = joiner.getJoinConfig();
    Iterable<String> requiredInputs = joinConfig.getRequiredInputs();
    if (Iterables.size(requiredInputs) == numOfInputs) {
      innerJoin(perStageJoinElements);
    }
  }

  private Map<String, List<JoinElement<INPUT_RECORD>>> getPerStageJoinElements() {
    Map<String, List<JoinElement<INPUT_RECORD>>> perStageJoinElements = new HashMap<>();
    while (iterator.hasNext()) {
      JoinElement<INPUT_RECORD> joinElement = iterator.next();
      String stageName = joinElement.getStageName();
      if (perStageJoinElements.get(stageName) == null) {
        perStageJoinElements.put(stageName, new ArrayList<JoinElement<INPUT_RECORD>>());
      }
      perStageJoinElements.get(stageName).add(joinElement);
    }
    return perStageJoinElements;
  }

  private void innerJoin(Map<String, List<JoinElement<INPUT_RECORD>>> perStageJoinElements) throws Exception {
    // As we get intersection of records from n stages in inner join, if the number of stages we got after reduce
    // are not same as number of stages we are joining, then there is nothing to emit.
    if (perStageJoinElements.size() != numOfInputs) {
      return;
    }

    List<List<JoinElement<INPUT_RECORD>>> list = new ArrayList<>(perStageJoinElements.values());
    ArrayList<JoinElement<INPUT_RECORD>> joinElements = new ArrayList<>();
    getCartesianProduct(list, 0, numOfInputs, joinElements);
  }

  // TODO use iterative algorithm instead of recursion
  private void getCartesianProduct(List<List<JoinElement<INPUT_RECORD>>> list, int index, int size,
                                   List<JoinElement<INPUT_RECORD>> joinElements) throws Exception {
    if (joinElements.size() == size) {
      emitter.emit(joiner.merge(joinKey, joinElements));
      return;
    }

    List<JoinElement<INPUT_RECORD>> joinElementList = list.get(index);
    for (int i = 0; i < joinElementList.size(); i++) {
      joinElements.add(joinElementList.get(i));
      getCartesianProduct(list, index + 1, size, joinElements);
      joinElements.remove(joinElements.size() - 1);
    }
  }
}
