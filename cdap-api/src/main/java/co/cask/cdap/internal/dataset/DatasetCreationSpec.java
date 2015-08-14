/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.dataset;

import co.cask.cdap.api.dataset.DatasetProperties;

/**
 * Information for creating dataset instance.
 */
public final class DatasetCreationSpec {
  private final String instanceName;
  private final String typeName;
  private final DatasetProperties props;

  public DatasetCreationSpec(String instanceName, String typeName, DatasetProperties props) {
    this.instanceName = instanceName;
    this.typeName = typeName;
    this.props = props;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public String getTypeName() {
    return typeName;
  }

  public DatasetProperties getProperties() {
    return props;
  }
}
