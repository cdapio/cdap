/*
 * Copyright 2016 Cask Data, Inc.
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
package co.cask.cdap.data2.dataset2.lib.cube;

import java.util.Map;

/**
 * Class to encapsulate dimension name aliases
 */
public class AggregationAlias {
  private final Map<String, String> dimensionAliasMap;

  public AggregationAlias(Map<String, String> dimensionAliasMap) {
    this.dimensionAliasMap = dimensionAliasMap;
  }

  /**
   * If dimension name has an alias return the alias name, else return the dimension name. This is useful when
   * associating dimension name in aggregation with a value of a different dimension.
   * Example for the aggregation group "workflow" the dimension name "run" in aggregation
   * should use the value of dimension "wfr" (workflow-run-id)
   * @param dimensionName
   * @return dimension name alias or dimension name
   */
  public String getAlias(String dimensionName) {
    return dimensionAliasMap.containsKey(dimensionName) ? dimensionAliasMap.get(dimensionName) : dimensionName;
  }
}
