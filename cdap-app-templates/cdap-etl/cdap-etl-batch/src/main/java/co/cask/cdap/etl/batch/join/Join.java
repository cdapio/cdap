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
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    Set<String> requiredInputs = Sets.newHashSet(joinConfig.getRequiredInputs());

    if (!perStageJoinElements.keySet().containsAll(requiredInputs)) {
      return;
    }

    join(perStageJoinElements, requiredInputs);
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

  private void join(Map<String, List<JoinElement<INPUT_RECORD>>> perStageJoinElements, Set<String> requiredInputs)
    throws Exception {
    List<List<JoinElement<INPUT_RECORD>>> list = new ArrayList<>(perStageJoinElements.values());
    ArrayList<JoinElement<INPUT_RECORD>> joinRow = new ArrayList<>();
    Set<String> joinRowInputs = new HashSet<>();
    getCartesianProduct(list, 0, joinRow, joinRowInputs, requiredInputs);
  }

  // TODO use iterative algorithm instead of recursion
  private void getCartesianProduct(List<List<JoinElement<INPUT_RECORD>>> list, int index,
                                   List<JoinElement<INPUT_RECORD>> joinRow,
                                   Set<String> joinRowInputs, Set<String> requiredInputs) throws Exception {
    // check till the end of the list and emit only if records from all the required inputs are present in joinElements
    if (index == list.size() && joinRowInputs.containsAll(requiredInputs)) {
      emitter.emit(joiner.merge(joinKey, joinRow));
      return;
    }

    if (index >= list.size()) {
      return;
    }

    for (JoinElement<INPUT_RECORD> joinElement : list.get(index)) {
      joinRow.add(joinElement);
      joinRowInputs.add(joinElement.getStageName());
      getCartesianProduct(list, index + 1, joinRow, joinRowInputs, requiredInputs);
      joinRow.remove(joinRow.size() - 1);
      joinRowInputs.remove(joinElement.getStageName());
    }
  }
}
