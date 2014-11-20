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
import co.cask.cdap.api.data.DataSetContext;
import com.google.common.collect.ImmutableMap;

import java.io.Closeable;
import java.util.Map;

/**
 * Helper to create DataSets
 */
public final class DataSets {

  public static Map<String, Closeable> createDataSets(DataSetContext context,
                                                      Iterable<String> datasetNames,
                                                      Map<String, String> arguments) {
    ImmutableMap.Builder<String, Closeable> builder = ImmutableMap.builder();

    for (String name : datasetNames) {
      Closeable dataset;
      if (arguments != null && !arguments.isEmpty()) {
        Map<String, String> datasetArguments = RuntimeArguments.
          selectScope("dataset", name, arguments);
        dataset = context.getDataSet(name, datasetArguments);
      } else {
        dataset = context.getDataSet(name);
      }
      if (dataset != null) {
        builder.put(name, dataset);
      }
    }
    return builder.build();
  }

  private DataSets() {}
}
