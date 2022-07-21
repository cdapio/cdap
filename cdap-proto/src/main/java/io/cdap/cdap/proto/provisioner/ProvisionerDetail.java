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

package io.cdap.cdap.proto.provisioner;

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A class which contains the config of a provisioner
 */
public class ProvisionerDetail {
  private final String name;
  private final String label;
  private final String description;
  @SerializedName("configuration-groups")
  private final List<Object> configurationGroups;
  @SerializedName("filters")
  private final List<Object> filters;
  @SerializedName("icon")
  private final Object icon;
  @SerializedName("beta")
  private final boolean beta;

  public ProvisionerDetail(String name, String label, String description, List<Object> configurationGroups,
                           @Nullable List<Object> filters, @Nullable Object icon, boolean beta) {
    this.name = name;
    this.label = label;
    this.description = description;
    this.configurationGroups = configurationGroups;
    this.filters = filters;
    this.icon = icon;
    this.beta = beta;
  }

  public String getName() {
    return name;
  }

  public String getLabel() {
    return label;
  }

  public String getDescription() {
    return description;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProvisionerDetail that = (ProvisionerDetail) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(label, that.label) &&
      Objects.equals(description, that.description) &&
      Objects.equals(configurationGroups, that.configurationGroups) &&
      Objects.equals(filters, that.filters) &&
      Objects.equals(icon, that.icon) &&
      Objects.equals(beta, that.beta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, label, description, configurationGroups, filters, icon, beta);
  }
}
