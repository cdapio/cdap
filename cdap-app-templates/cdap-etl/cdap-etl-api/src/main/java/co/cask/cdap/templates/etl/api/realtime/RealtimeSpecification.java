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

package co.cask.cdap.templates.etl.api.realtime;

import co.cask.cdap.api.Resources;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageSpecification;

import java.util.List;

/**
 * Contains the specification of the realtime source/sink stage, set during configuration.
 */
public class RealtimeSpecification extends StageSpecification {
  private final Resources resources;

  public RealtimeSpecification(String className, String name, String description, List<Property> properties,
                               Resources resources) {
    super(className, name, description, properties);
    this.resources = resources;
  }

  public Resources getResources() {
    return resources;
  }
}
