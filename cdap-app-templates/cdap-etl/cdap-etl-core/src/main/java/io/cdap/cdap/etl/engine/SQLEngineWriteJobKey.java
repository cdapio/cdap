/*
 * Copyright © 2022 Cask Data, Inc.
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
 * Class used to represent a write job for a given dataset.
 *
 * Since one dataset can be written to multiple destinations, we use this class as a way to ensure
 * we keep the dataset name consistent with our existing job keys.
 */
public class SQLEngineWriteJobKey extends SQLEngineJobKey {

  private final String destinationDatasetName;

  public SQLEngineWriteJobKey(String datasetName, String destinationDatasetName,
      SQLEngineJobType jobType) {
    super(datasetName, jobType);
    this.destinationDatasetName = destinationDatasetName;
  }

  public String getDestinationDatasetName() {
    return destinationDatasetName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    SQLEngineWriteJobKey that = (SQLEngineWriteJobKey) o;
    return Objects.equals(destinationDatasetName, that.destinationDatasetName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), destinationDatasetName);
  }
}
