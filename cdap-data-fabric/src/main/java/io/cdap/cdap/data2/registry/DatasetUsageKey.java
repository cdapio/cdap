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

package co.cask.cdap.data2.registry;

import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Objects;

/**
 * Key used to keep track of whether a particular usage has been recorded already or not (for UsageRegistry).
 */
public class DatasetUsageKey {
  private final DatasetId dataset;
  private final ProgramId owner;

  public DatasetUsageKey(DatasetId dataset, ProgramId owner) {
    this.dataset = dataset;
    this.owner = owner;
  }

  DatasetId getDataset() {
    return dataset;
  }

  ProgramId getOwner() {
    return owner;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatasetUsageKey that = (DatasetUsageKey) o;
    return Objects.equal(dataset, that.dataset) &&
      Objects.equal(owner, that.owner);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dataset, owner);
  }
}
