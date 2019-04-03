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
package co.cask.cdap.internal.app.runtime.artifact.plugin.p5;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.EndpointPluginContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Path;

/**
 * plugin has endpoint to aggregate data
 */
@Plugin(type = "interactive")
@Name("aggregator")
@Description("This is Plugin that aggregates data")
public class PluginWithPojo {

  @Path("aggregate")
  public Map<Long, Long> aggregateData(List<TestData> dataList, EndpointPluginContext pluginContext)
    throws Exception {
    Map<Long, Long> aggregateResult = new HashMap<>();
    for (TestData data : dataList) {
      long current =
        aggregateResult.containsKey(data.getTimeStamp()) ? aggregateResult.get(data.getTimeStamp()) : 0;
      aggregateResult.put(data.getTimeStamp(), current + data.getValue());
    }
    return aggregateResult;
  }

  // Endpoint which throws IllegalArgumentException
  @Path("throwException")
  public String throwException(String testString) throws Exception {
    throw new IllegalArgumentException("Invalid user inputs: " + testString);
  }
}
