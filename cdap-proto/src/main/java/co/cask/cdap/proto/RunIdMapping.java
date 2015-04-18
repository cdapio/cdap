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

package co.cask.cdap.proto;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * Mapping from Twill RunId to the CDAP RunId.
 */
public class RunIdMapping {
  @SerializedName("mapping")
  private final Map<String, String> runIdMapping;

  public RunIdMapping(Map<String, String> runIdMapping) {
    this.runIdMapping = runIdMapping;
  }

  public Map<String, String> getRunIdMapping() {
    return runIdMapping;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("runIdMapping", runIdMapping)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RunIdMapping that = (RunIdMapping) o;

    return runIdMapping.equals(that.runIdMapping);
  }

  @Override
  public int hashCode() {
    return runIdMapping.hashCode();
  }
}
