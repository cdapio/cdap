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

import java.util.Map;

/**
 * Specification of an adapter.
 *
 * @param <T> the type for the configuration of the adapter.
 */
public final class AdapterSpecification<T> {

  private final String name;
  private final String description;
  private final String template;
  // config object that will be interpreted differently by different templates.
  private final T config;

  /**
   * Construct an AdapterSpecification with the given parameters.
   *
   * @param name the name of the adapter
   * @param description the description of the adapter
   * @param template the template to base the adapter off of
   * @param config the config for the adapter
   */
  public AdapterSpecification(String name, String description, String template, T config) {
    this.name = name;
    this.description = description;
    this.template = template;
    this.config = config;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getTemplate() {
    return template;
  }

  public T getConfig() {
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

    AdapterSpecification that = (AdapterSpecification) o;

    return Objects.equal(name, that.name) &&
      Objects.equal(description, that.description) &&
      Objects.equal(template, that.template) &&
      Objects.equal(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, description, template, config);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("description", description)
      .add("template", template)
      .add("config", config)
      .toString();
  }
}
