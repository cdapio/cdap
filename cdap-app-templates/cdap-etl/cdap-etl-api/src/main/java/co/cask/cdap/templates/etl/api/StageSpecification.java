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

package co.cask.cdap.templates.etl.api;

import co.cask.cdap.api.ProgramSpecification;

import java.util.List;

/**
 * Contains the specification of the stage defined during the configuration.
 */
public class StageSpecification implements ProgramSpecification {
  private final String className;
  private final String name;
  private final String description;
  private final List<Property> properties;

  public StageSpecification(String className, String name, String description, List<Property> properties) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.properties = properties;
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  public List<Property> getProperties() {
    return properties;
  }
}
