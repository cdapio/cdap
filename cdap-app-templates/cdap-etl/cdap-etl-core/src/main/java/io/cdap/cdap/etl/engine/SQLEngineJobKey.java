/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.engine;

import java.util.Objects;

/**
 * Class used to represent an unique job for a given dataset.
 *
 * A job key is based on the dataset name and the SQL Engine Job Type.
 */
public class SQLEngineJobKey {
  private final String datasetName;
  private final SQLEngineJobType jobType;

  public SQLEngineJobKey(String datasetName, SQLEngineJobType jobType) {
    this.datasetName = datasetName;
    this.jobType = jobType;
  }

  public String getDatasetName() {
    return datasetName;
  }

  public SQLEngineJobType getJobType() {
    return jobType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SQLEngineJobKey that = (SQLEngineJobKey) o;
    return datasetName.equals(that.datasetName) && jobType == that.jobType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasetName, jobType);
  }
}
