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

package co.cask.cdap.etl.tool.config;

import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link ETLStage} as it existed in 3.2.x. The name and properties in this object were moved to the
 * {@link Plugin} class.
 */
public class OldETLStage {
  private final String name;
  private final Map<String, String> properties;
  private final String errorDatasetName;

  public OldETLStage(String name, Map<String, String> properties, @Nullable String errorDatasetName) {
    this.name = name;
    this.properties = properties;
    this.errorDatasetName = errorDatasetName;
  }

  public String getName() {
    return name;
  }

  public String getErrorDatasetName() {
    return errorDatasetName;
  }

  public Map<String, String> getProperties() {
    return properties;
  }
}
