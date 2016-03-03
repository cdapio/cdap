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
package co.cask.cdap.etl.batch;

/**
 *
 */

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.batch.AggregationFunction;

import java.util.HashMap;
import java.util.Map;

@Plugin(type = "aggregationFunction")
@Name("sum")
public class SumAggregation extends AggregationFunction<StructuredRecord, Number> {

  private final SumConfig config;
  private Number sum;

  public static co.cask.cdap.etl.common.Plugin getPlugin() {
    Map<String, String> properties = new HashMap<>();
    return new co.cask.cdap.etl.common.Plugin("SumAggregation", properties);
  }

  public static class SumConfig extends PluginConfig {
    private String column;
  }

  public SumAggregation(SumConfig config) {
    this.config = config;
  }

  @Override
  public void reset() {

  }

  @Override
  public void update(StructuredRecord record) {
    // get type of config.column, initialize sum to right type based on that
//    sum += (casted to correct thing) record.get(config.column);
  }

  @Override
  public Number aggregate() {
    return sum;
  }
}