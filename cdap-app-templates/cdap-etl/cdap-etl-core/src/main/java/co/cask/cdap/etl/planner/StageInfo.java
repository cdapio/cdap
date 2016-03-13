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

package co.cask.cdap.etl.planner;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Class to encapsulate information needed about a plugin at runtime.
 */
public class StageInfo {
  private final String name;
  private final String errorDatasetName;
  private final boolean isConnector;

  public StageInfo(String name, @Nullable String errorDatasetName, boolean isConnector) {
    this.name = name;
    this.errorDatasetName = errorDatasetName;
    this.isConnector = isConnector;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getErrorDatasetName() {
    return errorDatasetName;
  }

  public boolean isConnector() {
    return isConnector;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StageInfo that = (StageInfo) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(errorDatasetName, that.errorDatasetName) &&
      isConnector == that.isConnector;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, errorDatasetName, isConnector);
  }

  @Override
  public String toString() {
    return "StageInfo{" +
      "name='" + name + '\'' +
      ", errorDatasetName='" + errorDatasetName + '\'' +
      ", isConnector=" + isConnector +
      '}';
  }
}
