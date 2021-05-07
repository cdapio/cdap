/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.deploy.pipeline;

import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;

import java.util.Collection;

/**
 * ApplicationSpecification and associated system tables. This is used because the StructuredTableSpecifications
 * can't be stored in the ApplicationSpecification since the class is not in cdap-api.
 */
public class AppSpecInfo {
  private final ApplicationSpecification appSpec;
  private final Collection<StructuredTableSpecification> systemTables;
  private final Metadata metadata;

  public AppSpecInfo(ApplicationSpecification appSpec,
                     Collection<StructuredTableSpecification> systemTables, Metadata metadata) {
    this.appSpec = appSpec;
    this.systemTables = systemTables;
    this.metadata = metadata;
  }

  public ApplicationSpecification getAppSpec() {
    return appSpec;
  }

  public Collection<StructuredTableSpecification> getSystemTables() {
    return systemTables;
  }

  public Metadata getMetadata() {
    return metadata;
  }
}
