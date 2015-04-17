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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.api.Resources;
import co.cask.cdap.templates.etl.api.realtime.RealtimeConfigurer;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSpecification;

/**
 * Default implementation of {@link RealtimeConfigurer}.
 */
public class DefaultRealtimeConfigurer extends DefaultStageConfigurer implements RealtimeConfigurer {
  private Resources resources;

  public DefaultRealtimeConfigurer(Class klass) {
    super(klass);
    this.resources = new Resources();
  }

  @Override
  public void setResources(Resources resources) {
    this.resources = resources;
  }

  @Override
  public RealtimeSpecification createSpecification() {
    return new RealtimeSpecification(className, name, description, properties, resources);
  }
}
