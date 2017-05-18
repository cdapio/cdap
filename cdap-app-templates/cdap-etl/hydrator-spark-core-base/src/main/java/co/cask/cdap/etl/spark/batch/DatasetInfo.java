/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.spark.batch;

import co.cask.cdap.api.data.batch.Split;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Information required to read from or write to a dataset in the spark program.
 */
final class DatasetInfo {

  private final String datasetName;
  private final Map<String, String> datasetArgs;
  private final List<Split> splits;

  DatasetInfo(String datasetName, Map<String, String> datasetArgs, @Nullable List<Split> splits) {
    this.datasetName = datasetName;
    this.datasetArgs = ImmutableMap.copyOf(datasetArgs);
    this.splits = splits == null ? null : ImmutableList.copyOf(splits);
  }

  public String getDatasetName() {
    return datasetName;
  }

  public Map<String, String> getDatasetArgs() {
    return datasetArgs;
  }

  @Nullable
  public List<Split> getSplits() {
    return splits;
  }
}
