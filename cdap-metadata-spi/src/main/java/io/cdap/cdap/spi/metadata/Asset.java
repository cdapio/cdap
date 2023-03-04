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

package io.cdap.cdap.spi.metadata;

import io.cdap.cdap.api.annotation.Beta;
import java.util.Objects;

/**
 * Represents an Asset with a FQN which is a fully-qualified identifier. It refers to the data
 * source that is being read from or written into - e.g. BigQuery Dataset, DB Table, etc. FQN is
 * formed by using the plugin properties that together identifies an asset. For e.g. in case of DB
 * plugins, the plugin can be of the form {dbType}.{host}:{port}.{database}.{schema}.
 */
@Beta
public class Asset {

  private final String fqn;

  /**
   * Creates an instance of Asset.
   *
   * @param fqn fully-qualified name of the Asset.
   */
  public Asset(String fqn) {
    this.fqn = fqn;
  }

  /**
   * @return the fully-qualified name of the {@link Asset}.
   */
  public String getFQN() {
    return fqn;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Asset asset = (Asset) o;
    return fqn.equals(asset.fqn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fqn);
  }

  @Override
  public String toString() {
    return "Dataset{"
        + "fqn='" + fqn + '\''
        + '}';
  }
}
