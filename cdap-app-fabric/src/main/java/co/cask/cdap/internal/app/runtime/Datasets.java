/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.Dataset;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Helper to create Datasets.
 */
public final class Datasets {

  public static Map<String, Dataset> createDatasets(DatasetContext context,
                                                    Iterable<String> datasetNames,
                                                    Map<String, String> arguments) {

    ImmutableMap.Builder<String, Dataset> builder = ImmutableMap.builder();
    for (String name : datasetNames) {
      Dataset dataset;
      if (arguments != null && !arguments.isEmpty()) {
        Map<String, String> datasetArguments = RuntimeArguments.
          extractScope(Scope.DATASET, name, arguments);
        dataset = context.getDataset(name, datasetArguments);
      } else {
        dataset = context.getDataset(name);
      }
      builder.put(name, dataset);
    }
    return builder.build();
  }

  private Datasets() {}
}
