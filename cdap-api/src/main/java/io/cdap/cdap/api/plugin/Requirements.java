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

package io.cdap.cdap.api.plugin;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A class to encapsulate all the different types of requirements provided by
 * {@link io.cdap.cdap.api.annotation.Requirements}.
 */
public class Requirements {

  public static final Requirements EMPTY = new Requirements(Collections.emptySet(), Collections.emptySet());

  // currently this class only contains one set but we are using the object for storage during serialization so that we
  // can add more when needed in future
  private final Set<String> datasetTypes;

  //Accelerators associated with this plugin
  private final Set<String> accelerators;

  /**
   * Creates a {@link Requirements} object from the given {@link Set}. Note: Requirements are case insensitive and all
   * the requisites will be converted into lowercase.
   *
   * @param datasetTypes a {@link Set} containing dataset type requirements
   */
  public Requirements(Set<String> datasetTypes) {
    this(datasetTypes, Collections.emptySet());
  }

  public Requirements(Set<String> datasetTypes, Set<String> accelerators) {
    this.datasetTypes = datasetTypes.isEmpty() ? Collections.emptySet() :
      Collections.unmodifiableSet(datasetTypes.stream().map(String::toLowerCase).collect(Collectors.toSet()));
    this.accelerators = accelerators.isEmpty() ? Collections.emptySet() :
      Collections.unmodifiableSet(accelerators.stream().map(String::toLowerCase).collect(Collectors.toSet()));
  }

  /**
   * @return {@link Set} containing the dataset type requirement which can be be empty if there are no requirements
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
    Requirements that = (Requirements) o;
    return Objects.equals(datasetTypes, that.datasetTypes) && Objects.equals(accelerators, that.accelerators);
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasetTypes, accelerators);
  }

  @Override
  public String toString() {
    return "Requirements{" +
      "datasetTypes=" + datasetTypes +
      "accelerators=" + accelerators +
      '}';
  }

  public boolean isEmpty() {
    return datasetTypes.isEmpty() && accelerators.isEmpty();
  }
}
