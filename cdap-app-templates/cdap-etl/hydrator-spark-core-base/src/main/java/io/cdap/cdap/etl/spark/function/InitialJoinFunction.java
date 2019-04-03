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

package co.cask.cdap.etl.spark.function;

import co.cask.cdap.etl.api.JoinElement;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * Transforms an Object into a singleton list containing the JoinElement of that object. Used to map the initial
 * PairRDD of a join into the type expected by other parts of the join.
 *
 * @param <T> type of object
 */
public class InitialJoinFunction<T> implements Function<T, List<JoinElement<T>>> {
  private final String inputStageName;

  public InitialJoinFunction(String inputStageName) {
    this.inputStageName = inputStageName;
  }

  @Override
  public List<JoinElement<T>> call(T obj) throws Exception {
    List<JoinElement<T>> list = new ArrayList<>(1);
    list.add(new JoinElement<>(inputStageName, obj));
    return list;
  }
}
