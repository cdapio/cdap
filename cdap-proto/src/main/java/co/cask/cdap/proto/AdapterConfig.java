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

import co.cask.cdap.api.annotation.Beta;
import com.google.common.base.Objects;
import com.google.gson.JsonElement;

import javax.annotation.Nullable;

/**
 * Config of an adapter. This is the input for requests to add an adapter.
 */
@Beta
public final class AdapterConfig {
  private final String description;
  private final String template;
  private final JsonElement config;

  /**
   * Construct an AdapterConfig with the given parameters.
   *
   * @param description the description of the adapter
   * @param template the template to base the adapter off of
   * @param config the config for the adapter
   */
  public AdapterConfig(String description, String template, JsonElement config) {
    this.description = description;
    this.template = template;
    this.config = config;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  public String getTemplate() {
    return template;
  }

  @Nullable
  public JsonElement getConfig() {
    return config;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AdapterConfig that = (AdapterConfig) o;

    return Objects.equal(description, that.description) &&
      Objects.equal(template, that.template) &&
      Objects.equal(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(description, template, config);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("description", description)
      .add("template", template)
      .add("config", config)
      .toString();
  }
}
