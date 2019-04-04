/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A class to encapsulate all the different capabilities of a {@link Provisioner}
 */
public class Capabilities {

  public static final Capabilities EMPTY = new Capabilities(Collections.emptySet());

  private final Set<String> datasetTypes;

  /**
   * Creates a {@link Capabilities} object from the given {@link Set}. Note: Capabilities are case insensitive and all
   * the capabilities will be converted into lowercase.
   *
   * @param datasetTypes a {@link Set} containing dataset type capabilities
   */
  public Capabilities(Set<String> datasetTypes) {
    this.datasetTypes = datasetTypes.isEmpty() ? Collections.emptySet() :
      Collections.unmodifiableSet(datasetTypes.stream().map(String::toLowerCase).collect(Collectors.toSet()));
  }

  /**
   * @return {@link Set} containing the dataset type capabilities which can be be empty if there are no requirements
   */
  public Set<String> getDatasetTypes() {
    return datasetTypes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Capabilities that = (Capabilities) o;
    return Objects.equals(datasetTypes, that.datasetTypes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasetTypes);
  }

  @Override
  public String toString() {
    return "Capabilities{" +
      "datasetTypes=" + datasetTypes +
      '}';
  }

  public boolean isEmpty() {
    return datasetTypes.isEmpty();
  }
}
