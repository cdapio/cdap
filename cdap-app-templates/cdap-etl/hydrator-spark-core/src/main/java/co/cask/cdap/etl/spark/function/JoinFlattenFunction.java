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
import scala.Tuple2;

import java.util.List;

/**
 * Flattens the Tuple2 of list and object returned by a join into a single list.
 */
public class JoinFlattenFunction implements
  Function<Tuple2<List<JoinElement<Object>>, Object>, List<JoinElement<Object>>> {
  private final String inputStageName;

  public JoinFlattenFunction(String inputStageName) {
    this.inputStageName = inputStageName;
  }

  @Override
  public List<JoinElement<Object>> call(Tuple2<List<JoinElement<Object>>, Object> in) throws Exception {
    List<JoinElement<Object>> output = in._1();
    output.add(new JoinElement<>(inputStageName, in._2()));
    return output;
  }

}
